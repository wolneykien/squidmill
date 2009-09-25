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

(define (string-join lst sep)
  (let loop ((text "")
             (lst lst))
    (if (null? lst)
      text
      (loop (if (> (string-length text) 0)
              (string-append text sep (car lst))
              (car lst))
            (cdr lst)))))

(define (extract-domain uri)
  (if (and uri (> (string-length uri) 0))
    (let ((uri-list (string-tokenize uri '(#\/))))
      (if (and (>= (length uri-list) 2)
               (eq? #\: (string-ref (car uri-list)
                                    (- (string-length (car uri-list)) 1))))
        (cadr uri-list)
        uri))))

(define (bulk-insert db-fold-left bulk)
  (db-fold-left values #f
    (string-append
      "insert or ignore into access_log" " "
      (string-join bulk " union "))))

(define (sqlquote txtval)
  (string-append "'" txtval "'"))

(define (add-event bulk timestamp elapsed client action/code size method uri ident hierarchy/from content)
  (append bulk
          (list (string-append "select" " "
                  (string-join (list timestamp
                                     (sqlquote ident)
                                     (sqlquote (extract-domain uri))
                                     size
                                     elapsed)
                               ", ")))))

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
          (string-append "timestamp <= strftime('%s', 'now', '-" age-note))))
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
      (string-append "delete from access_log where "
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

(define (make-where-stm stime etime minsize maxsize ident-pat uri-pat)
  (string-append
    "where "
    (string-join
       (append '()
         (if stime
           (list (string-join "timestamp > strftime('%s', '"
                              stime "', 'utc')"))
           '())
         (if etime
           (list (string-join "timestamp <= strftime('%s', '"
                              etime "', 'utc')"))
           '())
         (if minsize
           (list (string-join "sum(size) > "
                              (number->string minsize)))
           '())
         (if maxsize
           (list (string-join "sum(size) <= "
                              (number->string maxsize)))
           '())
         (if (and ident-pat (> (string-length ident-pat) 0))
           (list (string-join "ident is like '%" ident-pat
                              "%'"))
           '())
         (if (and uri-pat (> (string-length uri-pat) 0))
           (list (string-join "uri is like '%" uri-pat
                              "%'"))
           '()))
       " and ")))

(define (make-union-select select-stm . tail-smts)
  (string-join
    (list (string-append select-stm " access_log "
                         (string-join tail-smts " "))
          (string-append select-stm " hourly_log "
                         (string-join tail-smts " "))
          (string-append select-stm " daily_log "
                         (string-join tail-smts " "))
          (string-append select-stm " monthly_log "
                         (string-join tail-smts " ")))
    " union "))

(define (make-limit-stm limit)
  (if (and limit (>= limit 0))
    (string-append "limit " (number->string limit))
    ""))

(define (report-on-users db-fold-left out-proc seed
                         stime etime minsize maxsize ident-pat limit)
  (let ((select-stm
          (string-append
            "select strftime('d%.%m.%Y %H:%M:%S', max(timestamp), 'localtime'), "
                   "ident, sum(size), sum(elapsed) from"))
        (where-stm (make-where-stm stime etime minsize maxsize
                                   ident-pat #f))
        (group-stm "group by ident"))
    (db-fold-left out-proc seed
      (string-append
        (make-union-select select-stm where-stm group-stm)
        "order by 3 desc, 2 asc, 1 desc "
        (make-limit-stm limit)))))

(define (report-on-uris db-fold-left out-proc seed
                        stime etime minsize maxsize ident-pat limit)
  (let ((select-stm
          (string-append
            "select strftime('d%.%m.%Y %H:%M:%S', max(timestamp), 'localtime'), "
                   "uri, sum(size), sum(elapsed) from"))
        (where-stm (make-where-stm stime etime minsize maxsize #f
                                   ident-pat))
        (group-stm "group by uri"))
    (db-fold-left out-proc seed
      (string-append
        (make-union-select select-stm where-stm group-stm)
        "order by 3 desc, 2 asc, 1 desc "
        (make-limit-stm limit)))))

(define (make-out-proc out-proc seed limit)
  (lambda (seed . cols)
    (call-with-values
      (lambda () seed)
      (lambda (out-seed count more)
        (if (<= count limit)
          (values #t
                 (values (apply out-proc out-seed cols)
                         (+ count 1)
                         #f))
          (values #f (values out-seed count #t)))))))

(define (report db-fold-left out-proc seed report-selector limit
                . query-parameters)
  (apply report-selector
         db-fold-left
         (make-out-proc out-proc limit)
         (values seed 0 #f)
         query-parameters
         (and limit (+ limit 1))))

(define (s-report-output seed timestamp id size elapsed)
  (write (list timestamp id (string->number size)
               (string->number elapsed)))
  (newline)
  seed)

(define (make-text-report-output sep)
  (lambda (seed timestamp id size elapsed)
    (string-join
      (list timestamp id (string->number size)
            (string->number elapsed))
      sep)))

(define (process-log proc port)
  (let loop ((ln (read-line port))
             (bulk '()))
    (if (not (eof-object? ln))
      (loop (read-line port)
            (apply proc bulk (string-tokenize ln '(#\space #\tab #\newline))))
      bulk)))

(define (call-with-input filename proc)
  (if (equal? filename "-")
    (proc (current-input-port))
    (call-with-input-file filename proc)))

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

(define (add-logs db-fold-left bulk-size . files)
  (for-each
    (lambda (file)
      (call-with-input file
        (lambda (port)
          (let ((bulk (process-log (make-add-event db-fold-left
                                                   bulk-size)
                                   port)))
            (if (not (null? bulk))
              (bulk-insert db-fold-left bulk))))))
     files))

(define (scan-args . command-line)
  (let ((input-files '())
        (db-name "squidmill.db")
        (bulk-size 256)
        (sdate #f)
        (edate #f)
        (ident-pat #f)
        (uri-pat #f)
        (minsize #f)
        (maxsize #f))
    (let scan-next ((args command-line))
      (if (null? args)
        (append (list db-name bulk-size) input-files)
        (if (and (> (string-length (car args)) 1)
                 (eq? (string-ref (car args) 0) #\-))
          (case (string->symbol (car args))
            ((-d) (set! db-name (cadr args))
                  (scan-next (cddr args)))
            ((-B) (set! bulk-size (string->number (cadr args)))
                  (scan-next (cddr args)))
            ((-s) (set! sdate (cadr args))
                  (scan-next (cddr args)))
            ((-e) (set! edate (cadr args))
                  (scan-next (cddr args)))
            ((-m) (set! minsize (string->number (cadr args)))
                  (scan-next (cddr args)))
            ((-M) (set! maxsize (string->number (cadr args)))
                  (scan-next (cddr args)))
            ((-i) (set! ident-pat (cadr args))
                  (scan-next (cddr args)))
            ((-u) (set! uri-pat (cadr args))
                  (scan-next (cddr args)))
            (else (usage)
                  (exit 0)))
          (begin
            (set! input-files (append input-files (car args)))
            (scan-next (cdr args))))))))

(define (main . command-line)
  (let scan-args ((input-files '())
                  (db-name "squidmill.db")
                  (bulk-size 256)
                  (args command-line))
    (if (null? args)
        (call-with-values
          (lambda () (sqlite3 db-name))
          (lambda (db-fold-left db-close)
            (with-exception-catcher
              (lambda (e)
                (db-close)
                (raise e))
              (lambda ()
                (init-db db-fold-left)
                (apply add-logs db-fold-left bulk-size input-files)
                (db-close)))))
        (if (and (> (string-length (car args)) 1)
                 (eq? (string-ref (car args) 0) #\-))
            (case (string->symbol (car args))
              ((-d) (scan-args input-files (cadr args) bulk-size
                               (cddr args)))
              ((-B) (scan-args input-files db-name
                               (string->number (cadr args))
                               (cddr args)))
              (else (error "Unknown option: " (car args))))
            (scan-args (append input-files (list (car args)))
                       db-name bulk-size (cdr args))))))
