
(define (fold-right kons knil clist1)
  (let f ((list1 clist1))
    (if (null? list1)
        knil
        (kons (car list1) (f (cdr list1))))))

(define (report-exception ex)
  (if (and (pair? ex) (eq? (car ex) 'sqlite3-err))
      (begin
	(display "SQLite3 error (" (current-error-port))
	(display (cadr ex) (current-error-port))
	(display "): " (current-error-port))
	(display (caddr ex) (current-error-port))
	(newline (current-error-port)))
      (begin
	(display ex (current-error-port))
	(newline (current-error-port)))))

(define (report-and-raise ex)
  (report-exception ex)
  (raise ex))

(define (string-tokenize txtval charset)
  (fold-right (lambda (w tail)
                (if (null? w)
                    tail
                    (cons (list->string w) tail)))
              '()
              (fold-right (lambda (c tail)
                            (if (member c charset)
                                (if (null? (car tail))
                                    tail
                                    (cons '() tail))
                                (cons (cons c (car tail))
                                      (cdr tail))))
                          '(())
                          (string->list txtval))))

(define (make-string-join sep)
  (lambda lst
    (let loop ((text "")
               (lst lst))
      (if (null? lst)
        text
        (loop (if (car lst)
                (if (> (string-length text) 0)
                  (string-append text sep (car lst))
                  (car lst))
                text)
              (cdr lst))))))

(define (extract-domain uri)
  (if (and uri (> (string-length uri) 0))
    (let ((uri-list (string-tokenize uri '(#\/))))
      (if (and (>= (length uri-list) 2)
               (eq? #\: (string-ref (car uri-list)
                                    (- (string-length (car uri-list)) 1))))
        (cadr uri-list)
        uri))))


(define *db-mutex*
  (make-mutex 'db-mutex))

(define (make-db-fold-left-thread-safe db-fold-left)
  (lambda (fn seed stm)
    (with-exception-catcher
      (lambda (e)
        (mutex-unlock! *db-mutex*)
        (report-and-raise e))
      (lambda ()
        (mutex-lock! *db-mutex*)
        (let ((res (db-fold-left fn seed stm)))
          (mutex-unlock! *db-mutex*)
          res)))))

(define-macro (db-fold-left-debug fn seed stm)
  `(let ((debug-stm ,stm))
     (pp debug-stm)
     (db-fold-left ,fn ,seed debug-stm)))

(define (stub . args)
  (values #f #f))

(define *max-retries* 10)

(define (make-db-fold-left-retry-on-busy db-fold-left)
  (lambda (fn seed stm)
    (let try ((t 1))
      (with-sqlite3-exception-catcher
       (lambda (code msg . args)
	 (if (and (< t *max-retries*)
		  (eq? code 5))
	     (begin
	       (thread-sleep! 0.5)
	       (try (+ t 1)))
	     (apply raise-sqlite3-error code msg args)))
       (lambda ()
	 (db-fold-left fn seed
		       (string-append stm
				      (if (> t 1)
					  (string-append " /* try "
							 (number->string t)
							 " */")
					  ""))))))))

(define (db-begin-immediate db-fold-left)
  (db-fold-left stub #f "begin immediate"))

(define (db-begin-deferred db-fold-left)
  (db-fold-left stub #f "begin deferred"))

(define (db-commit db-fold-left)
  (db-fold-left stub #f "commit"))

(define (db-rollback db-fold-left)
  (db-fold-left stub #f "rollback"))

(define (with-transaction db-fold-left begin-proc commit-proc rollback-proc thunk)
  (begin-proc db-fold-left)
  (with-exception-catcher
   (lambda (e)
     (rollback-proc db-fold-left)
     (raise e))
   (lambda ()
     (let ((res (thunk)))
       (commit-proc db-fold-left)
       res))))

(define (bulk-insert db-fold-left bulk)
  (with-transaction db-fold-left db-begin-immediate db-commit db-rollback
    (lambda ()
      (db-fold-left stub #f
        (string-append
          "insert or ignore into access_log" " "
          (apply (make-string-join " union ") bulk))))))

(define (sqlquote txtval)
  (string-append "'" txtval "'"))

(define (add-event bulk timestamp elapsed client action/code size method uri ident hierarchy/from content)
  (append bulk
          (list (string-append "select" " "
                  ((make-string-join ", ")
                     timestamp
                     (if (or (not ident)
                             (equal? "-" ident))
                       (sqlquote client)
                       (sqlquote ident))
                     (sqlquote (extract-domain uri))
                     size
                     elapsed)))))

(define (init-table db-fold-left table-name)
  (db-fold-left stub #f
    (string-append
      "create table if not exists " table-name " "
      "(timestamp double, ident text, uri text, size integer, "
       "elapsed long)"))
  (db-fold-left stub #f
    (string-append
      "create unique index if not exists "
      table-name "_timestamp_ident "
      "on " table-name " (timestamp desc, ident asc)")))

(define (table-exists? db-fold-left table-name)
  (db-fold-left
    (lambda (seed name)
      (values #f (equal? name table-name)))
    #f
    (string-append "select name from sqlite_master where type='table' and name='" table-name "'")))

(define (init-db db-fold-left)
  (if (not (table-exists? db-fold-left "access_log"))
    (init-table db-fold-left "access_log"))
  (if (not (table-exists? db-fold-left "hourly_log"))
    (init-table db-fold-left "hourly_log"))
  (if (not (table-exists? db-fold-left "daily_log"))
    (init-table db-fold-left "daily_log"))
  (if (not (table-exists? db-fold-left "monthly_log"))
    (init-table db-fold-left "monthly_log")))

(define (round-log db-fold-left from-table to-table age-note time-template)
  (with-transaction db-fold-left db-begin-immediate db-commit db-rollback
    (lambda ()
      (let ((threshold-condition
            (string-append "timestamp <= strftime('%s', 'now', '-"
                           age-note
                           "')")))
      (db-fold-left stub #f
        (string-append
          "insert or replace into " to-table " "
          "select min(timestamp), ident, uri, sum(size), sum(elapsed) "
          "from " from-table " "
          "where " threshold-condition " "
          "group by strftime('" time-template "', timestamp, 'unixepoch'), "
          "ident, uri "
          "order by 1 desc"))
      (db-fold-left stub #f
        (string-append "delete from " from-table " where "
                       threshold-condition))))))

(define (log->hourly db-fold-left)
  (round-log db-fold-left "access_log" "hourly_log"
              "1 day" "%Y-%m-%d %H"))

(define (hourly->daily db-fold-left)
  (round-log db-fold-left "hourly_log" "daily_log"
              "1 month" "%Y-%m-%d"))

(define (daily->monthly db-fold-left)
  (round-log db-fold-left "daily_log" "monthly_log"
              "1 year" "%Y-%m"))

(define (round-all-logs db-fold-left)
  (log->hourly db-fold-left)
  (hourly->daily db-fold-left)
  (daily->monthly db-fold-left))

(define *rounder* #f)
(define (init-rounder period db-fold-left)
  (if (not *rounder*)
    (begin
      (set! *rounder*
        (make-thread
          (lambda ()
            (let loop ()
              (thread-sleep! period)
              (round-all-logs db-fold-left)
              (loop)))
          "rounder"))
      (thread-start! *rounder*))))

(define (rounder-foreground)
  (if *rounder*
    (thread-join! *rounder*)))

(define (terminate-rounder)
  (if *rounder*
    (thread-terminate! *rounder*)))

(define (make-where-stm stime etime ident-pat uri-pat)
  (if (or stime etime (and ident-pat
                           (not (eq? #t ident-pat))
                           (> (string-length ident-pat) 0))
                      (and uri-pat
                           (not (eq? #t uri-pat))
                           (> (string-length uri-pat) 0)))
    (string-append
      "where "
      ((make-string-join " and ")
         (and stime
             (string-append "timestamp > strftime('%s', '"
                            stime "', 'utc')"))
         (and etime
              (string-append "timestamp <= strftime('%s', '"
                             etime "', 'utc')"))
         (and ident-pat
              (not (eq? #t ident-pat))
              (> (string-length ident-pat) 0)
              (string-append "ident glob '" ident-pat "'"))
         (and uri-pat
              (not (eq? #t uri-pat))
              (> (string-length uri-pat) 0)
              (string-append "uri glob '" uri-pat "'"))))
    ""))

(define (make-union-select select-stm . tail-stms)
  ((make-string-join " union ")
    (string-append select-stm " access_log "
                   (apply (make-string-join " ") tail-stms))
    (string-append select-stm " hourly_log "
                   (apply (make-string-join " ") tail-stms))
    (string-append select-stm " daily_log "
                   (apply (make-string-join " ") tail-stms))
    (string-append select-stm " monthly_log "
                   (apply (make-string-join " ") tail-stms))))

(define (make-limit-stm limit)
  (if (and limit (>= limit 0))
    (string-append "limit " (number->string limit))
    ""))

(define (make-select-stm stime etime minsize maxsize ident-pat uri-pat)
  (string-append
    "select max(timestamp) as timestamp,"
    " total(size) as size, total(elapsed) as elapsed"
    (if ident-pat ", ident" "")
    (if uri-pat ", uri" "")
    " from"))

(define (make-group-stm ident-pat uri-pat)
  (string-append
    (if (or ident-pat uri-pat) "group by " "")
    ((make-string-join ", ")
       (and ident-pat "ident")
       (and uri-pat "uri"))))

(define (make-order-stm ident-pat uri-pat)
  ((make-string-join ", ")
     "order by 2 desc, 1 desc"
     (and ident-pat "ident asc")
     (and uri-pat "uri asc")))

(define (make-out-proc out-proc seed limit)
  (lambda (seed . cols)
    (call-with-values
      (lambda () seed)
      (lambda (out-seed count)
        (if (or (not limit) (< count limit))
          (values #t
                 (values (apply out-proc out-seed cols)
                         (+ count 1)))
          (values #f #f))))))

(define (report db-fold-left out-proc seed
                stime etime minsize maxsize ident-pat uri-pat
                limit summary)
  (let ((select-stm (make-select-stm
                      stime etime minsize maxsize ident-pat uri-pat))
        (where-stm (make-where-stm stime etime ident-pat uri-pat))
        (group-stm (make-group-stm ident-pat uri-pat))
        (order-stm (make-order-stm ident-pat uri-pat))
        (limit-stm (make-limit-stm (and limit (+ limit 1)))))
    (let ((stm ((make-string-join " ")
                  "select"
                  ((make-string-join ", ")
                     "strftime('%d.%m.%Y %H:%M:%S'"
                     (if summary "max(timestamp)" "timestamp")
                     "'unixepoch', 'localtime')"
                     (if summary "total(size), total(elapsed)"
                                 "size, elapsed")
                     (and (not summary) ident-pat "ident")
                     (and (not summary) uri-pat "uri"))
                  "from ("
                  (make-select-stm stime etime minsize maxsize
                                   ident-pat uri-pat)
                  "("
                  (make-union-select select-stm where-stm group-stm)
                  ") as log"
                  group-stm
                  ") as res_log"
                  (if (or minsize maxsize)
                    (string-append
                      " where "
                      ((make-string-join " and ")
                        (and minsize (string-append
                                       "size > "
                                       (number->string minsize)))
                        (and maxsize (string-append
                                       "size <= "
                                       (number->string maxsize)))))
                    "")
                  (and (not summary) order-stm)
                  (and (not summary) limit-stm))))
      (with-transaction db-fold-left db-begin-deferred db-rollback db-rollback
        (lambda ()
	  (db-fold-left
            (make-out-proc out-proc seed limit)
            (values seed 0)
            stm))))))

(define (s-report-output seed . cols)
  (write cols)
  (newline)
  seed)

(define (make-text-report-output sep)
  (lambda (seed . cols)
    (display
      (apply (make-string-join sep)
        (map (lambda (a)
               (cond
                 ((string? a) a)
                 ((integer? a)
                  (number->string (if (exact? a) a (inexact->exact a))))
                 (else (object->string a))))
          cols)))
    (newline)
    seed))

(define (process-log proc port)
  (let ((bulk '()))
    (with-exception-catcher
      (lambda (e)
        (raise (cons e bulk)))
      (lambda ()
        (let loop ((ln (read-line port)))
          (if (not (eof-object? ln))
            (begin
              (set! bulk
                (apply proc
                  bulk
                  (string-tokenize ln '(#\space #\tab #\newline))))
              (loop (read-line port)))))))
    bulk))

(define (call-with-input filename follow proc)
  (if (equal? filename "-")
    (proc (current-input-port))
    (if (not follow)
      (call-with-input-file filename proc)
      (call-with-input-process
        (list path: "tail"
              arguments: (list "-F" filename))
        proc))))

(define (make-add-event db-fold-left bulk-size)
  (let ((last-timestamp "")
        (last-ident "")
        (tail 0))
    (lambda (bulk timestamp elapsed client action/code size method uri ident . other-fields)
      (let ((bulk (if (>= (length bulk) bulk-size)
                    (begin
                      (bulk-insert db-fold-left bulk)
                      '())
                    bulk)))
        (let ((bulk
                (if (and (equal? ident last-ident)
                         (equal? timestamp last-timestamp))
                  (begin
                    (set! tail (+ tail 1))
                    (apply add-event
                           bulk
                           (string-append timestamp
                                          (number->string tail))
                           elapsed client action/code size method
                           uri ident
                           other-fields))
                  (begin
                    (set! tail 0)
                      (apply add-event bulk timestamp elapsed client
                             action/code size method uri ident
                             other-fields)))))
          (set! last-timestamp timestamp)
          (set! last-ident ident)
          bulk)))))

(define (add-logs db-fold-left bulk-size follow . files)
  (for-each
    (lambda (file)
      (call-with-input file follow
        (lambda (port)
          (let ((bulk 
                   (with-exception-catcher
                     (lambda (e)
                       (if (not (or (null? (cdr e))
                                    (sqlite3-error? (car e))))
                         (bulk-insert db-fold-left (cdr e)))
                       (if follow
                         (send-signal (process-pid port)
                                      (if (signal-exception? (car e))
                                        (cdar e)
                                        2)))
                       (raise (car e)))
                     (lambda ()
                       (process-log (make-add-event db-fold-left
                                                    bulk-size)
                                    port)))))
              (if (not (null? bulk))
                (bulk-insert db-fold-left bulk))))))
       files))

(define (opt-key? arg)
  (and (> (string-length arg) 1)
       (eq? (string-ref arg 0) #\-)))

(define (version)
  "2.0.0")

(define (usage)
  (display
    ((make-string-join "\n")
      (string-append "Squidmill v" (version))
      "Usage: squidmill [log files] [options]"
      " "
      "General options:"
      "    -d DB-FILE        Database file name"
      "    -h                Print this screen"
      "    -D                Debug mode on"
      " "
      "Update options:"
      "    -B NUMBER         Bulk-insert size (default is 256)"
      "    -F                Follow mode"
      " "
      "Rounding options:"
      "    -R [PERIOD]       Round old data to save space (and reporting time)."
      "                      Do rounding every PERIOD mins, if specified"
      " "
      "Reporting options:"
      "    -r [FORMAT]       Report format. Default is plaintext."
      "                      Use 'list' for Scheme list."
      "    -s YYYY-DD-MM     Select records newer than that"
      "    -e YYYY-DD-MM     Select records not newer than that"
      "    -m NUMBER         Exclude trafic statistic not more than that"
      "    -M NUMBER         Exclude trafic statistic more than that"
      "    -i [PATTERN]      Count statistic for individual users filtering"
      "                      them optionally"
      "    -u [PATTERN]      Count statistic for individual URIs filtering"
      "                      them optionally"
      "    -S                Calculate final summary"
      "    -l NUMBER         Limit report by that number of rows"))
  (newline))

(define (scan-args . command-line)
  (let ((input-files '())
        (db-name "squidmill.db")
        (bulk-size 256)
        (follow #f)
        (sdate #f)
        (edate #f)
        (ident-pat #f)
        (uri-pat #f)
        (minsize #f)
        (maxsize #f)
        (limit #f)
        (round-data #f)
        (report #f)
        (summary #f)
        (debug #f))
    (let scan-next ((args command-line))
      (if (null? args)
        (append (list db-name bulk-size follow sdate edate ident-pat
                      uri-pat minsize maxsize limit round-data report
                      summary debug)
                input-files)
        (if (opt-key? (car args))
          (case (string->symbol (substring (car args) 1 2))
            ((d) (set! db-name (cadr args))
                  (scan-next (cddr args)))
            ((B) (set! bulk-size (string->number (cadr args)))
                  (scan-next (cddr args)))
            ((s) (set! sdate (cadr args))
                  (scan-next (cddr args)))
            ((e) (set! edate (cadr args))
                  (scan-next (cddr args)))
            ((m) (set! minsize (string->number (cadr args)))
                  (scan-next (cddr args)))
            ((M) (set! maxsize (string->number (cadr args)))
                  (scan-next (cddr args)))
            ((i) (if (or (null? (cdr args))
                          (opt-key? (cadr args)))
                    (begin
                      (set! ident-pat #t)
                      (scan-next (cdr args)))
                    (begin
                      (set! ident-pat (cadr args))
                      (scan-next (cddr args)))))
            ((u) (if (or (null? (cdr args))
                          (opt-key? (cadr args)))
                    (begin
                      (set! uri-pat #t)
                      (scan-next (cdr args)))
                    (begin
                      (set! uri-pat (cadr args))
                      (scan-next (cddr args)))))
            ((l) (set! limit (string->number (cadr args)))
                  (scan-next (cddr args)))
            ((R) (if (or (null? (cdr args))
                          (opt-key? (cadr args)))
                    (begin
                      (set! round-data #t)
                      (scan-next (cdr args)))
                    (begin
                      (set! round-data (string->number (cadr args)))
                      (scan-next (cddr args)))))
            ((r) (if (or (null? (cdr args))
                          (opt-key? (cadr args)))
                    (begin
                      (set! report #t)
                      (scan-next (cdr args)))
                    (begin
                      (set! report (string->symbol (cadr args)))
                      (scan-next (cddr args)))))
            ((F) (set! follow #t)
                 (scan-next (cdr args)))
            ((S) (set! summary #t)
                 (scan-next (cdr args)))
            ((D) (set! debug #t)
                 (scan-next (cdr args)))
            (else (usage)
                  (exit 0)))
          (begin
            (set! input-files (append input-files (list (car args))))
            (scan-next (cdr args))))))))

(define (do-report db-fold-left report-format . report-args)
  (apply report
    (append
      (list db-fold-left)
      (case report-format
        ((list) (list s-report-output #f))
         (else (list (make-text-report-output "\t") #f)))
      report-args)))

(define (make-db-fold-left-debug db-fold-left)
  (lambda (fn seed stm)
    (db-fold-left-debug fn seed stm)))

(define (main db-name bulk-size follow sdate edate ident-pat
              uri-pat minsize maxsize limit round-data report-format
              summary debug . input-files)
  (call-with-values
    (lambda ()
      (sqlite3 db-name))
    (lambda (db-fold-left db-close)
      (let ((db-fold-left
              (make-db-fold-left-retry-on-busy
                (if debug
                  (make-db-fold-left-debug
                    (make-db-fold-left-thread-safe db-fold-left))
                  db-fold-left)))
            (db-close db-close))
	(with-exception-catcher
	  (lambda (e)
	    (db-close)
	    (raise e))
	  (lambda ()
	    (init-db db-fold-left)
	    (if (and round-data (not (eq? round-data #t)))
		(init-rounder (* round-data 60) db-fold-left))
	    (if (not (null? input-files))
		(apply add-logs db-fold-left bulk-size follow input-files))
	    (if (eq? round-data #t) (round-all-logs db-fold-left))
	    (if (or (not report-format)
		    (do-report db-fold-left report-format
			       sdate edate minsize maxsize
			       ident-pat uri-pat limit summary))
		(begin
		  (rounder-foreground)
		  (db-close)
		  (exit 0))
		(begin
		  (db-close)
		  (exit 100)))))))))

(signal-set-exception! *SIGHUP*)
(signal-set-exception! *SIGTERM*)
(signal-set-exception! *SIGINT*)
(signal-set-exception! *SIGQUIT*)

(with-exception-catcher
  (lambda (e)
    (if (signal-exception? e)
      (begin
        (terminate-rounder)
        (exit 0))
      (begin
        (terminate-rounder)
        (report-and-raise e))))
    (lambda ()
      (let ((args
              (with-exception-catcher
                (lambda (e) #f)
                (lambda () (apply scan-args (cdr (command-line)))))))
        (if args
          (apply main args)
          (begin
            (usage)
            (exit 1))))))
