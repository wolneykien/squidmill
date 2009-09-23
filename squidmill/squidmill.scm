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
      "insert or replace into access_log" " "
      (string-join bulk " union "))))

(define (add-event bulk timestamp elapsed client action/code size method uri ident hierarchy/from content)
  (append bulk
          (list (string-append "select" " "
                  (string-join (list timestamp
                                     ident
                                     (extract-domain uri)
                                     size
                                     elapsed)
                               ", ")))))

(define (init-db db-fold-left)
  (db-fold-left values #f
    "create table if not exists access_log (timestamp double, ident text, uri text, size integer, elapsed long)")
  (db-fold-left values #f
    "create unique index if not exists access_log_timestamp_ident on access_log (timestamp desc, ident asc)"))

(define (process-log proc port)
  (let loop ((ln (read-line port))
             (bulk '()))
    (if (not (eof-object? ln))
      (begin
        (loop (read-line port)
              (apply proc bulk (string-tokenize ln '(#\space #\tab #\newline))))))))

(define (call-with-input filename proc)
  (cond
   ((equal? filename "-")
    (proc (current-input-port)))
   ((file-exists? filename)
    (call-with-input-file filename proc))
   (else (make-table))))

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
          (process-log (make-add-event db-fold-left bulk-size) port))))
     files))

(define (main . command-line)
  (let scan-args ((input-files '())
             (db-name "squidmill.db")
             (bulk-size 10)
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
              ((-d) (scan-args (input-files (cadr args) (cddr args))))
              (else (error "Unknown option: " (car args))))
            (scan-args (append input-files (list (car args)))
                       db-name (cdr args))))))
