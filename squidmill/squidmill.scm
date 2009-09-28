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
                     (sqlquote ident)
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
  (if (or stime etime minsize maxsize ident-pat uri-pat)
    (string-append
      "where "
      ((make-string-join " and ")
         (and stime
             (string-append "timestamp > strftime('%s', '"
                            stime "', 'utc')"))
         (and etime
              (string-append "timestamp <= strftime('%s', '"
                             etime "', 'utc')"))
         (and minsize
              (string-append "sum(size) > " (number->string minsize)))
         (and maxsize
              (string-append "sum(size) <= " (number->string maxsize)))
         (and ident-pat (> (string-length ident-pat) 0)
              (string-append "ident is like '%" ident-pat "%'"))
         (and uri-pat (> (string-length uri-pat) 0)
              (string-append "uri is like '%" uri-pat "%'"))))))

(define (make-union-select select-stm . tail-smts)
  ((make-string-join " union ")
    (string-append select-stm " access_log "
                   (apply (make-string-join " ") tail-smts)))
    (string-append select-stm " hourly_log "
                   (apply (make-string-join " ") tail-smts))
    (string-append select-stm " daily_log "
                   (apply (make-string-join " ") tail-smts))
    (string-append select-stm " monthly_log "
                   (apply (make-string-join " ") tail-smts)))

(define (make-limit-stm limit)
  (if (and limit (>= limit 0))
    (string-append "limit " (number->string limit))
    ""))

(define (make-select-stm stime etime minsize maxsize ident-pat uri-pat)
  (string-append
    "select strftime('d%.%m.%Y %H:%M:%S', max(timestamp), 'localtime'),"
    (if ident-pat " ident," "")
    (if uri-pat " uri," "")
    " sum(size), sum(elapsed)"
    " from"))

(define (make-group-stm ident-pat uri-pat)
  (string-append
    (if (or ident-pat uri-pat) "group by " "")
    ((make-string-join ", ")
       (and ident-pat "ident")
       (and uri-pat "uri"))))

(define (make-order-stm ident-pat uri-pat)
  ((make-string-join ", ")
     "order by 1 desc"
     (and ident-pat "ident asc")
     (and uri-pat "uri asc")))

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

(define (report db-fold-left out-proc seed
                stime etime minsize maxsize ident-pat uri-pat
                limit)
  (let ((select-stm (make-select-stm stime etime minsize maxsize
                                     ident-pat uri-pat))
        (where-stm (make-where-stm stime etime minsize maxsize
                                   ident-pat uri-pat))
        (group-stm (make-group-stm ident-pat uri-pat))
        (order-stm (make-order-stm ident-pat uri-pat))
        (limit-stm (make-limit-stm (and limit (+ limit 1)))))
    (db-fold-left
      (make-out-proc out-proc limit)
      (values seed 0 #f)
      ((make-string-join " ")
         (make-union-select select-stm where-stm group-stm)
         group-stm order-stm limit-stm))))

(define (s-report-output seed timestamp id size elapsed)
  (write (list timestamp id (string->number size)
               (string->number elapsed)))
  (newline)
  seed)

(define (make-text-report-output sep)
  (lambda (seed timestamp id size elapsed)
    ((make-string-join sep) timestamp id (string->number size)
                            (string->number elapsed))))

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

(define (opt-key? arg)
  (and (> (string-length arg) 1)
       (eq? (string-ref arg 0) #\-)))

(define (scan-args . command-line)
  (let ((input-files '())
        (db-name "squidmill.db")
        (bulk-size 256)
        (sdate #f)
        (edate #f)
        (ident-pat #f)
        (uri-pat #f)
        (minsize #f)
        (maxsize #f)
        (limit #f)
        (round-data #f)
        (report #f))
    (let scan-next ((args command-line))
      (if (null? args)
        (append (list db-name bulk-size sdate edate ident-pat
                      uri-pat minsize maxsize limit round-data report)
                input-files)
        (if (opt-key? (car args))
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
            ((-i) (if (or (null? (cdr args))
                          (opt-key? (cadr args)))
                    (begin
                      (set! ident-pat "%")
                      (scan-next (cdr args)))
                    (begin
                      (set! ident-pat (cadr args))
                      (scan-next (cddr args)))))
            ((-u) (if (or (null? (cdr args))
                          (opt-key? (cadr args)))
                    (begin
                      (set! uri-pat "%")
                      (scan-next (cdr args)))
                    (begin
                      (set! uri-pat (cadr args))
                      (scan-next (cddr args)))))
            ((-l) (set! limit (string->number (cadr args)))
                  (scan-next (cddr args)))
            ((-R) (set! round-data #t)
                  (scan-next (cdr args)))
            ((-r) (if (or (null? (cdr args))
                          (opt-key? (cadr args)))
                    (begin
                      (set! report #t)
                      (scan-next (cdr args)))
                    (begin
                      (set! report (string->symbol (cadr args)))
                      (scan-next (cddr args)))))
            (else (usage)
                  (exit 0)))
          (begin
            (set! input-files (append input-files (car args)))
            (scan-next (cdr args))))))))

(define (main db-name bulk-size sdate edate ident-pat
              uri-pat minsize maxsize limit round-data report-format
              . input-files)
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
            (apply add-logs db-fold-left bulk-size input-files))
          (if round-data (round-all-logs db-fold-left))
          (if report-format
            (apply report
              (append
                (list db-fold-left)
                (case report
                  ((list) (list s-report-output #f))
                  (else (list (make-text-report-output "\t") #f)))
                (list sdate edate minsize maxsize ident-pat uri-pat
                      limit))))
          (db-close))))))
