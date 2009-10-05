(define (fold-right kons knil clist1)
  (let f ((list1 clist1))
    (if (null? list1)
        knil
        (kons (car list1) (f (cdr list1))))))

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

(define-macro (db-fold-left-debug fn seed stm)
  `(let ((debug-stm ,stm))
     (pp debug-stm)
     (db-fold-left ,fn ,seed debug-stm)))

(define (bulk-insert db-fold-left bulk)
  (db-fold-left values #f
    (string-append
      "insert or ignore into access_log" " "
      (apply (make-string-join " union ") bulk))))

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
  (db-fold-left values #f
    (string-append
      "create table if not exists " table-name " "
      "(timestamp double, ident text, uri text, size integer, "
       "elapsed long)"))
  (db-fold-left values #f
    (string-append
      "create unique index if not exists "
      table-name "_timestamp_ident "
      "on " table-name " (timestamp desc, ident asc)")))

(define (init-db db-fold-left)
  (init-table db-fold-left "access_log")
  (init-table db-fold-left "hourly_log")
  (init-table db-fold-left "daily_log")
  (init-table db-fold-left "monthly_log"))

(define (round-log db-fold-left from-table to-table age-note time-template)
  (db-fold-left values #f "begin exclusive transaction")
  (let ((threshold-condition
          (string-append "timestamp <= strftime('%s', 'now', '-"
                         age-note
                         "')")))
    (db-fold-left values #f
      (string-append
        "insert or replace into " to-table " "
        "select min(timestamp), ident, uri, sum(size), sum(elapsed) "
        "from " from-table " "
        "where " threshold-condition " "
        "group by strftime('" time-template "', timestamp, 'unixepoch'), "
        "ident, uri "
        "order by 1 desc"))
    (db-fold-left values #f
      (string-append "delete from " from-table " where "
                     threshold-condition)))
  (db-fold-left values #f "commit transaction"))

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

(define (make-where-stm stime etime ident-pat uri-pat)
  (if (or stime etime (and ident-pat (not (eq? #t ident-pat)))
                      (and uri-pat (not (eq? #t  uri-pat))))
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
                  (and (not summary) order-stm limit-stm))))
      (db-fold-left
        (make-out-proc out-proc seed limit)
        (values seed 0)
        stm))))

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
                       (if (not (null? (cdr e)))
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
      " "
      "Update options:"
      "    -B NUMBER         Bulk-insert size (default is 256)"
      "    -F                Follow mode"
      " "
      "Rounding options:"
      "    -R                Round old data to save space (and reporting time)"
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
        (summary #f))
    (let scan-next ((args command-line))
      (if (null? args)
        (append (list db-name bulk-size follow sdate edate ident-pat
                      uri-pat minsize maxsize limit round-data report
                      summary)
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
            ((R) (set! round-data #t)
                  (scan-next (cdr args)))
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

(define (main db-name bulk-size follow sdate edate ident-pat
              uri-pat minsize maxsize limit round-data report-format
              summary . input-files)
  (call-with-values
    (lambda () (sqlite3 db-name))
    (lambda (db-fold-left db-close)
      (with-exception-catcher
        (lambda (e)
          (db-close)
          (raise e))
        (lambda ()
          (init-db db-fold-left)
          (if (not (null? input-files))
            (apply add-logs db-fold-left bulk-size follow input-files))
          (if round-data (round-all-logs db-fold-left))
          (if (let ((ret (or (not report-format)
                         (do-report db-fold-left report-format
                                    sdate edate minsize maxsize
                                    ident-pat uri-pat limit summary))))
                (db-close)
                ret)
            (exit 0)
            (exit 100)))))))

(signal-set-exception! *SIGHUP*)
(signal-set-exception! *SIGTERM*)
(signal-set-exception! *SIGINT*)
(signal-set-exception! *SIGQUIT*)

(with-exception-catcher
  (lambda (e)
    (if (signal-exception? e)
      (exit 0)
      (raise e)))
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
