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
  (write (log->list timestamp elapsed client action/code size method uri ident hierarchy/from content))
  (newline)
  tail)

(define (table-update! key update-proc new-tbl old-tbl)
  (let ((new-val (table-ref new-tbl key #f))
        (old-val (table-ref old-tbl key #f)))
    (cond
     ((not new-val) old-tbl)
     ((not old-val)
      (table-set! old-tbl key new-val))    
     (else
      (table-set! old-tbl key (update-proc new-val old-val))))))

(define (sum-log-update! vals sum)
  (table-update! 'elapsed + vals sum)
  (table-update! 'size + vals sum)
  (table-update! 'timestamp min vals sum))

;; (define (sum-log sum timestamp elapsed client action/code size method uri ident hierarchy/from content)
;;   (let ((vals (list->table (log->list timestamp elapsed client action/code size method uri ident hierarchy/from content))))
;;     (cond
;;      ((table? sum)
;;       (table-update! 'elapsed + vals sum)
;;       (table-update! 'size + vals sum)
;;       (table-update! 'timestamp min vals sum)
;;       sum)
;;      (else vals))))

(define (personal-sum-log sum timestamp elapsed client action/code size method uri ident hierarchy/from content)
  (let ((vals (list->table (log->list timestamp elapsed client action/code size method uri ident hierarchy/from content)))
        (id (or (false-empty ident)
                (false-empty client))))
    (cond
     ((table? sum)
      (cond
       ((table-ref sum id #f) =>
        (lambda (psum)
          (sum-log-update! psum)
          psum))
       (else
        (table-set! sum id vals)))
      sum)
     (else
      (list->table (list (cons id vals)))))))

(define (sum-log sum timestamp elapsed client action/code size method uri ident hierarchy/from content)
  (personal-sum-log (personal-sum-log sum timestamp elapsed client action/code size method uri ident hierarchy/from content)
                    timestamp elapsed #f action/code size method uri #f hierarchy/from content))

(define (process-log proc port)
  (let loop ((tail #f))
    (let ((ln (read-line port)))
      (if (not (eof-object? ln))
          (loop (apply proc tail (string-tokenize ln)))
          tail))))

(define (sum-logs . files)
  (let ((sum (list->table '())))
    (for-each     
     (lambda (file)
       (sum-log-update!
        (let* ((l (string-length file))
               (t (and (>= l 4)
                       (substring file (- l 4) l))))
          (call-with-input-file file
            (lambda (port)
              (if (and t (equal? t ".scm"))
                  (read port)
                  (process-log sum-log port)))))
          sum))
     files)
    sum))

(write (map (lambda (entry)
              (cons (car entry)
                    (table->list (cdr entry))))
            (table->list (apply sum-logs (cdr (command-line))))))
(newline)
