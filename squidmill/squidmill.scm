(cond-expand
 ((not fold-right)
  (define (fold-right kons knil clist1)
    (let f ((list1 clist1))
      (if (null? list1)
          knil
          (kons (car list1) (f (cdr list1))))))))

(cond-expand
 ((not string-tokenize)
  (define (string-tokenize txtval)
    (fold-right (lambda (w tail)
                  (if (null? w)
                      tail
                      (cons (list->string w) tail)))
                '()
                (fold-right (lambda (c tail)
                              (if (or (eq? c #\space)
                                      (eq? c #\tab)
                                      (eq? c #\newline))
                                  (if (null? (car tail))
                                      tail
                                      (cons '() tail))
                                  (cons (cons c (car tail))
                                        (cdr tail))))
                            '(())
                            (string->list txtval))))))

(define (false-empty val)
  (and (not (equal? "-" val)) val))

(define (false-empty-number val)
  (and (not (equal? "-" val)) (string->number val)))

;; (define (report-log-line tail timestamp elapsed client action/code size method uri ident hierarchy/from content)
;;   (format #t "Timestamp: ~a~%" timestamp)
;;   (format #t "Elapsed: ~a~%" elapsed)
;;   (format #t "Client: ~a~%" client)
;;   (format #t "Action/Code: ~a~%" action/code)
;;   (format #t "Size: ~a~%" size)
;;   (format #t "Method: ~a~%" method)
;;   (format #t "URI: ~a~%" uri)
;;   (format #t "Ident: ~a~%" (false-empty ident))
;;   (format #t "Hierarch/From: ~a~%" hierarchy/from)
;;   (format #t "Content: ~a~%" content)
;;   (newline))

;; (define (report-uri tail timestamp elapsed client action/code size method uri ident hierarchy/from content)
;;   (format #t "URI: ~a~%" uri))

(define (log->list timestamp elapsed client action/code size method uri ident hierarchy/from content)
  (list
    (cons 'timestamp (false-empty-number timestamp))
    (cons 'elapsed (false-empty-number elapsed))
    (cons 'client client)
    (cons 'action/code action/code)
    (cons 'size (false-empty-number size))
    (cons 'method method)
    (cons 'uri uri)
    (cons 'ident (false-empty ident))
    (cons 'hierarchy/from hierarchy/from)
    (cons 'content content)))

(define (list-report tail timestamp elapsed client action/code size method uri ident hierarchy/from content)
  (write (log->assoc timestamp elapsed client action/code size method uri ident hierarchy/from content))
  (newline)
  tail)

(define (table-update! key update-proc new-tbl old-tbl)
  (let ((new-val (table-ref new-tbl key #f))
        (old-val (table-ref old-vals key #f)))
    (cond
     ((not new-val) old-tbl)
     ((not old-val)
      (table-set! old-tbl key new-val))    
     (else
      (table-set! old-tbl key (update-proc new-val old-val))))))

(define (sum-log sum timestamp elapsed client action/code size method uri ident hierarchy/from content)
  (let ((vals (log->assoc timestamp elapsed client action/code size method uri ident hierarchy/from content)))
    (cond
     ((table? sum)
      (table-update! 'elapsed + vals sum)
      (table-update! 'size + vals sum)
      (table-update! 'timestamp min vals sum)
      sum)
     (else
      (list->table vals)))))

(define (personal-sum-log sum timestamp elapsed client action/code size method uri ident hierarchy/from content)
  (let ((vals (log->assoc timestamp elapsed client action/code size method uri ident hierarchy/from content))
        (id (or (false-empty ident)
                (false-empty client))))
    (if (table? sum)
        (table-set! sum id
                    (cond
                     ((and (table? sum)
                           (table-ref sum id #f)) =>
                      (lambda (psum)
                        (table-update! 'elapsed + vals psum)
                        (table-update! 'size + vals psum)
                        (table-update! 'timestamp min vals psum)
                        psum))
                     (else (list->table vals))))
        (list->table (list (cons id
                                 (list->table vals)))))))

(define (process-log . args)
  (let ((proc (or (and (not (null? args))
                       (procedure? (car args))
                       (car args))
                  list-report))
        (files (or (and (not (null? args))
                        (or (and (string? (car args)) args)
                            (and (string? (cdr args)) (list (cdr args)))
                            (and (not (null? (cdr args)))
                                 (string? (cadr args)) (cdr args))))
                   (list "/var/log/squid/access.log"))))
    (let next-file ((files files)
                    (tail #f))
      (if (not (null? files))
          (next-file (cdr files)
                     (call-with-input-file (car files)
                       (lambda (port)
                         (let loop ((tail tail))
                           (let ((ln (read-line port)))
                             (if (not (eof-object? ln))
                                 (loop (apply proc tail (string-tokenize ln)))
                                 tail))))))
          tail))))

(write (process-log personal-sum-log))
(newline)
