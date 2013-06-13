;; Squid proxy server access log collector with rounding support
;;
;; Copyright (C) 2013 Paul Wolneykien <manowar@altlinux.org>
;;
;; This program is free software: you can redistribute it and/or modify
;; it under the terms of the GNU General Public License as published by
;; the Free Software Foundation, either version 3 of the License, or
;; (at your option) any later version.
;;
;; This program is distributed in the hope that it will be useful,
;; but WITHOUT ANY WARRANTY; without even the implied warranty of
;; MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
;; GNU General Public License for more details.
;;
;; You should have received a copy of the GNU General Public License
;; along with this program.  If not, see <http://www.gnu.org/licenses/>.

(c-declare "
#include <errno.h>
#include <string.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
")

(define lasterror
  (c-lambda () int
    "___result = errno;"))

(define lasterror->string
  (c-lambda (int) char-string
    "___result = strerror (___arg1);"))

(define daemon
  (c-lambda (int int) int
    "___result = daemon (___arg1, ___arg2);"))

(define getpid
  (c-lambda () int
    "___result = getpid ();"))

(c-define (stat->list dev ino mode uid gid atime mtime ctime)
	  (long long int int int long long long) scheme-object "stat2list" ""
  (list dev ino mode uid gid atime mtime ctime))

(c-define (empty-list) () scheme-object "empty_list" ""
  '())

(define stat
  (c-lambda (char-string) scheme-object
#<<c-lambda-end
struct stat sb;

if (stat (___arg1, &sb) == 0) {
  ___result = stat2list (sb.st_dev, sb.st_ino, sb.st_mode, sb.st_uid, sb.st_gid, sb.st_atime, sb.st_mtime, sb.st_ctime);
} else {
  ___result = empty_list ();
}
c-lambda-end
  ))

(define (raise-lasterror)
  (let* ((code (lasterror))
	 (message (lasterror->string code)))
    (error message code)))

(define *debug* #f)

(define (fold-right kons knil clist1)
  (let f ((list1 clist1))
    (if (null? list1)
        knil
        (kons (car list1) (f (cdr list1))))))

(define (display-error prefix code message . args)
  (let ((port (or (and (not (null? args)) (car args))
		  (current-error-port))))
    (if prefix
	(display prefix port)
	(display "Error" port))
    (if code
	(begin
	  (display " (" port)
	  (display code port)
	  (display ")" port)))
    (if message
      (begin
	(display ": " port)
	(display message port)))
    (newline port)
    (force-output port 1)))

(define (display-message prefix-message . args)
  (let ((code (and (not (null? args)) (car args)))
	(message (and (not (null? args))
		      (not (null? (cdr args)))
		      (cadr args)))
	(other-args (if (and (not (null? args))
			     (not (null? (cdr args))))
			(cddr args)
			'())))
    (apply display-error prefix-message code message other-args)))

(define (debug-message prefix-message . args)
  (if *debug*
    (apply display-message prefix-message args)))

(define (report-exception ex . args)
  (let ((port (or (and (not (null? args)) (car args))
		  (current-error-port))))
    (cond
     ((sqlite3-error? ex)
      (display-error "SQLite3 error"
		     (sqlite3-error-code ex)
		     (sqlite3-error-message ex)
		     port))
     ((domain-socket-exception? ex)
      (display-error "Socket error"
		     (domain-socket-exception-code ex)
		     (domain-socket-exception-message ex)
		     port))
     ((signal-exception? ex)
      (display-error "Signal received"
		     (signal-exception-number ex)
		     #f
		     port))
     ((error-exception? ex)
      (let ((message (error-exception-message ex))
	    (args (error-exception-parameters ex)))
	(display-error #f (and (not (null? args))
			       (car args))
		          message
			  port)))
     (else
      (display-exception ex port)))))

(define (report-and-raise ex)
  (report-exception ex)
  (raise ex))

(define (report-and-ignore ex)
  (report-exception ex))

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
     (pp debug-stm (current-error-port))
     (force-output (current-error-port) 1)
     (db-fold-left ,fn ,seed ,stm)))

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

(define *db-mutex*
  (make-mutex 'db-mutex))

(define (with-transaction db-fold-left begin-proc commit-proc rollback-proc thunk)
  (with-exception-catcher
    (lambda (e)
      (mutex-unlock! *db-mutex*)
      (raise e))
    (lambda ()
      (mutex-lock! *db-mutex*)
      (begin-proc db-fold-left)
      (with-exception-catcher
        (lambda (e)
	  (rollback-proc db-fold-left)
	  (raise e))
	(lambda ()
	  (let ((res (thunk)))
	    (commit-proc db-fold-left)
	    (mutex-unlock! *db-mutex*)
	    res))))))

(define union-join
  (make-string-join " union "))

(define (bulk-insert db-fold-left bulk)
  (with-transaction db-fold-left db-begin-immediate db-commit db-rollback
    (lambda ()
      (db-fold-left stub #f
        (string-append
          "insert or ignore into access_log" " "
          (apply union-join bulk))))))

(define (make-bulk-insert db-fold-left maxrows)
  (let ((row-count (and maxrows
			(rowcount db-fold-left "access_log"))))
    (lambda (db-fold-left bulk)
      (if (not (null? bulk))
	(begin
	  (if (and row-count (> (+ row-count (length bulk)) maxrows))
	    (begin
	      (debug-message "Row count in 'access_log' has exceeded the limit" #f row-count)
	      (round-all-logs db-fold-left maxrows)
	      (set! row-count (rowcount db-fold-left "access_log"))
	      (debug-message "Row count in 'access_log'" #f row-count)
	      (if (>= row-count maxrows)
		  (error "Rounding failed to reduce the number of rows in 'access_log'"))))
	  (bulk-insert db-fold-left bulk)
	  (if row-count
	    (set! row-count (+ row-count (length bulk)))))))))

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
  (debug-message "Round the database data" #f
		 (string-append from-table " -> " to-table))
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

(define (rowcount db-fold-left table-name)
  (db-fold-left
    (lambda (seed row-count)
      (values #f row-count))
    #f
    (string-append "select count(*) from " table-name)))


(define (round-all-logs db-fold-left maxrows)
  (if (or (not maxrows)
	  (>= (rowcount db-fold-left "access_log") maxrows))
    (log->hourly db-fold-left))
  (if (or (not maxrows)
	  (>= (rowcount db-fold-left "hourly_log") maxrows))
    (hourly->daily db-fold-left))
  (if (or (not maxrows)
	  (>= (rowcount db-fold-left "daily_log") maxrows))
    (daily->monthly db-fold-left)))

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

(define *read-log-delay* 0.01)

(define (process-log add-event port)
  (let loop ((bulk #f)
	     (ln (read-line port)))
    (if (not (eof-object? ln))
      (let ((bulk (apply add-event
			 bulk
			 (string-tokenize ln '(#\space #\tab #\newline)))))
	(if (null? bulk)
	  bulk
	  (loop bulk (read-line port))))
      bulk)))

(define (make-add-event db-fold-left bulk-insert bulk-size)
  (lambda (bulk timestamp elapsed client action/code size method uri ident . other-fields)
    (let ((bulk (apply add-event (or bulk '()) timestamp elapsed client
		       action/code size method uri ident other-fields)))
      (if (>= (length bulk) bulk-size)
	  (begin
	    (bulk-insert db-fold-left bulk)
	    '())
	  bulk))))

(define (open-input-file-or-ignore path existing-port)
  (with-exception-catcher
   (lambda (e)
     (if (no-such-file-or-directory-exception? e)
       (begin
	 (if existing-port
	   (debug-message "File disappeared" #f path))
	 #f)
       (raise e)))
   (lambda ()
     (if (equal? path "-")
       (current-input-port)
       (let ((port (open-input-file path)))
	 (debug-message "Open file" #f path)
	 port)))))

(define *reopen-delay* 0.1)
(define *read-delay* 0.01)

(define (make-add-log db-fold-left bulk-size maxrows)
  (let* ((bulk-insert (make-bulk-insert db-fold-left maxrows))
	 (add-event (make-add-event db-fold-left bulk-insert bulk-size)))
    (lambda (port)
      (and port
	   (let ((bulk (process-log add-event port)))
	     (and bulk
		  (or (null? bulk)
		      (and (bulk-insert db-fold-left bulk) #t))))))))

(define (close-or-report port path)
  (if (and port (not (equal? (current-input-port) port)))
    (with-exception-catcher report-and-ignore
      (lambda ()
	(if path
	  (debug-message "Close file" #f path))
	(close-port port)))))

(define (follow-add-logs db-fold-left bulk-size maxrows . files)
  (if (not (null? files))
    (debug-message "Follow the files until interrupted"))
  (let ((add-log (make-add-log db-fold-left bulk-size maxrows))
	(inputs (map (lambda (file)
		       (let ((file-stat (stat file))
			     (file-port (open-input-file-or-ignore file #f)))
			 (list (cons file file-stat)
			       file-port
			       0
			       (and (not (null? file-stat)) file-port))))
		     files)))
    (with-exception-catcher
      (lambda (e)
	(for-each
	 (lambda (input)
	   (apply (lambda (file port timestamp accessible)
		    (close-or-report port (car file)))
		  input))
	 inputs)
	(raise e))
      (lambda ()
	(let loop-inputs ((relax #t)
			  (res-inputs '())
			  (inputs inputs))
	  (if (null? inputs)
	    (begin
	      (if relax
		(thread-sleep! *read-delay*))
	      (if (not (null? res-inputs))
		(loop-inputs #t '() res-inputs)))
	    (apply
	      (lambda (file port timestamp accessible)
		(if (add-log port)
		  (begin
		    (if (not accessible)
		      (debug-message "File become accessible" #f (car file)))
		    (loop-inputs
		     #f
		     (append res-inputs
			     (list
			      (list file
				    port
				    (time->seconds (current-time))
				    #t)))
		     (cdr inputs)))
		  (loop-inputs
		   relax
		   (append res-inputs
			   (list
			    (let ((now (time->seconds (current-time))))
			      (if (> (- now timestamp) *reopen-delay*)
				(let ((new-stat (stat (car file))))
				  (if (and (not (null? new-stat))
					   (or (null? (cdr file))
					       (not (= (cadr file) (car new-stat)))
					       (not (= (caddr file) (cadr new-stat)))))
				    (let ((new-port (and (close-or-report port (car file))
							 (open-input-file-or-ignore (car file) port))))
				      (if (and (not accessible)
					       (not (null? new-stat)) new-port)
					(debug-message "File become accessible" #f (car file)))
				      (list (cons (car file) new-stat)
					    new-port
					    now
					    (and (not (null? new-stat)) new-port)))
				    (if (and accessible
					     (null? new-stat)
					     (not (null? (cdr file))))
				      (begin
					(debug-message "File become inaccessible" #f (car file))
					(list file port now #f))
				      (list file port now accessible))))
				(list file port timestamp accessible)))))
		   (cdr inputs))))
	      (car inputs))))))))

(define (add-logs db-fold-left bulk-size follow maxrows . files)
  (debug-message "Add logs from files" #f
		 (if (not (null? files))
		   (fold-right (lambda (file tail)
				 (if tail
				   (string-append file " " tail)
				   file))
			       #f
			       files)
		   "(no files)"))
  (if follow
    (apply follow-add-logs db-fold-left bulk-size maxrows files)
    (let ((add-log (make-add-log db-fold-left bulk-size maxrows)))
      (for-each
        (lambda (file)
	  (let ((port (open-input-file-or-ignore file #f)))
	    (if port
	      (with-exception-catcher
	        (lambda (e)
		  (close-or-report port file)
		  (raise e))
		(lambda ()
		  (let loop ()
		    (if (add-log port)
		      (loop)))
		  (close-or-report port file))))))
	files))))

(define (opt-key? arg)
  (and (> (string-length arg) 1)
       (eq? (string-ref arg 0) #\-)))

(define (version)
  "2.0.0")

(define *default-pidfile* "/var/run/squidmill.pid")

(define (usage)
  (display
    ((make-string-join "\n")
      (string-append "Squidmill v" (version))
      "Usage: squidmill [log files] [options]"
      " "
      "General options:"
      "    -d DB-FILE        Database file name"
      "    -c PATH           Path to the communication socket"
      "    -h                Print this screen"
      "    -D                Debug mode on"
      "    -b [PIDFILE]      Detach and run in the background"
      "                      Default pid-file is /var/run/squidmill.pid"
      "    -L LOG-FILE       Write the messages to that file instead of stderr"
      " "
      "Update options:"
      "    -B NUMBER         Read/insert bulk size (default is 1)"
      "    -F                Follow mode"
      " "
      "Rounding options:"
      "    -R [MAXROWS]      Round old data to save space (and reporting time)."
      "                      Do rounding for every MAXROWS records, if specified"
      " "
      "Reporting options:"
      "    -r [FORMAT]       Report format. Default is plaintext."
      "                      Use 'list' for Scheme list"
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
        (db-name #f)
        (socket-path #f)
        (bulk-size 1)
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
	(background #f)
	(log-file #f)
        (debug #f))
    (let scan-next ((args command-line))
      (if (null? args)
        (append (list db-name socket-path bulk-size follow sdate edate ident-pat
                      uri-pat minsize maxsize limit round-data report
                      summary debug background log-file)
                input-files)
        (if (opt-key? (car args))
          (case (string->symbol (substring (car args) 1 2))
            ((d) (set! db-name (cadr args))
                  (scan-next (cddr args)))
            ((c) (set! socket-path (cadr args))
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
            ((b) (if (or (null? (cdr args))
			 (opt-key? (cadr args)))
                    (begin
                      (set! background *default-pidfile*)
                      (scan-next (cdr args)))
                    (begin
                      (set! background (cadr args))
                      (scan-next (cddr args)))))
	    ((L) (set! log-file (cadr args))
	         (scan-next (cddr args)))
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

(define *socket-backlog* 100)
(define *socket-timeout* 500)

(define (close-all . args)
  (for-each (lambda (arg)
	      (if arg
		(with-exception-catcher report-and-ignore
                  (lambda ()
		    (cond
		     ((thread? arg)
		      (debug-message "Stop the " #f (thread-name arg))
		      (thread-send arg #t))
		     ((domain-socket? arg)
		      (debug-message "Close the server socket" #f (domain-socket-path arg))
		      (delete-domain-socket arg))
		     ((port? arg)
		      (debug-message "Close a file or a socket")
		      (close-port arg))
		     ((procedure? arg)
		      (arg)))))))
	    args))

(define (adjust-db-fold-left db-fold-left debug)
  (make-db-fold-left-retry-on-busy
    (if debug
      (make-db-fold-left-debug db-fold-left)
      db-fold-left)))

(define (make-ipc-db-fold-left socket)
  (lambda (fn seed stm)
    (if *debug*
      (pp stm (current-error-port)))
    (write stm socket)
    (newline socket)
    (force-output socket 1)
    (let loop ((seed seed) (row (read socket)))
      (if (string? row)
	(error (string-append "Server error: " row))
	(if (and (list? row) (not (null? row)))
	  (call-with-values
	    (lambda ()
	      (apply fn seed row))
	    (lambda (continue? new-seed)
	      (if (not continue?)
		new-seed
		(loop new-seed (read socket))))))))))

(define *no-client-delay* 0.02)

(define (send-error client e)
  (write (call-with-output-string ""
	   (lambda (string-port)
	     (report-exception e string-port)))
	 client)
  (force-output client 1))

(define (init-sql-server db-fold-left socket)
  (make-thread
   (lambda ()
     (debug-message "Waiting for a client to connect...")
     (let a-loop ((client (domain-socket-accept socket 0)))
       (if (and client (port? client))
	 (with-exception-catcher
	   (lambda (e)
	     (send-error client e)
	     (debug-message "Close client socket")
	     (close-or-report clinet)
	     (raise e))
	   (lambda ()
	     (debug-message "Client connected")
	     (thread-start!
	      (make-thread
	       (lambda ()
		 (with-exception-catcher
		   (lambda (e)
		     (send-error client e)
		     (debug-message "Close client socket")
		     (close-or-report client)
		     (report-exception e))
		   (lambda ()
		     (let r-loop ((stm (read client)))
		       (if (not (eof-object? stm))
			 (begin
			   (db-fold-left
			     (lambda (seed . args)
			       (write args client)
			       (newline client)
			       (force-output client 1)
			       (values #t seed))
			     #f
			     stm)
			   (r-loop (read client)))))
		     (close-port client)
		     (debug-message "Client disconnected"))))
	       (string-append "client "
			      (number->string (time->seconds (current-time))))))
	     (debug-message "Instance started. Waiting for an other client to connect...")))
	 (if (not (thread-receive 0 #f))
	   (thread-sleep! *no-client-delay*)))
       (if (not (thread-receive 0 #f))
	 (a-loop (domain-socket-accept socket 0)))))
   "SQL server"))

(define (detach pidfile-name)
  (if (= 0 (daemon 1 1))
    (let ((pid (getpid)))
      (debug-message "Process detached" pid)
      (if pidfile-name
	(with-exception-catcher
	  (lambda (e)
	    (display-message "Unable to write the PID-file")
	    (raise e))
	  (lambda ()
	    (with-output-to-file `(path: ,pidfile-name create: #t)
	      (lambda ()
		(display pid)
		(newline)))
	    (debug-message "PID-file" #f pidfile-name))))
      pid)
    (raise-lasterror)))

(define (delete-pidfile pidfile-name)
  (if (and pidfile-name (string? pidfile-name))
    (if (file-exists? pidfile-name)
      (begin
	(display-message "Delete the PID-file" #f pidfile-name)
	(delete-file pidfile-name)))))

(define (do-main db-name socket-path bulk-size follow sdate edate ident-pat
              uri-pat minsize maxsize limit round-data report-format
              summary debug . input-files)
  (call-with-values
    (lambda ()
      (let ((socket
	     (and socket-path
		  (with-exception-catcher
		    (lambda (e)
		      (if (and (domain-socket-exception? e)
			       (= 98 (domain-socket-exception-code e)))
			(with-exception-catcher
			  (lambda (e)
			    (if (and (domain-socket-exception? e)
				     (or (= 2 (domain-socket-exception-code e))
					 (= 111 (domain-socket-exception-code e)))
				     db-name)
			      (begin
				(if debug
				  (report-exception e))
				#f)
			      (raise e)))
			  (lambda ()
			    (let ((client-socket
				   (domain-socket-connect socket-path *socket-timeout*)))
			      (if client-socket
				(debug-message "Open client socket" #f socket-path))
			      client-socket)))
			(raise e)))
		    (lambda ()
		      (let ((server-socket
			     (make-domain-socket socket-path *socket-backlog*)))
			(if server-socket
			  (debug-message "Open server socket" #f socket-path))
			server-socket))))))
        (with-exception-catcher
          (lambda (e)
            (close-all socket)
            (raise e))
          (lambda ()
            (if (and (or (not socket) (domain-socket? socket)) db-name)
	      (begin
		(debug-message "Open database" #f db-name)
		(receive (db-fold-left db-close) (sqlite3 db-name)
	          (let ((db-fold-left (adjust-db-fold-left db-fold-left debug)))
		    (values db-fold-left db-close socket))))
	      (if (and socket (port? socket))
		(values (make-ipc-db-fold-left socket)
			(lambda ()
			  (debug-message "Close client socket" #f socket-path)
			  (close-port socket))
			#f)
		(values #f #f #f)))))))
    (lambda (db-fold-left db-close socket)
      (let* ((db-at-hand (and db-fold-left (or socket (not socket-path))))
	     (sql-server (and socket db-fold-left
			      (if db-at-hand
				(init-sql-server db-fold-left socket)
				(begin
				  (display-error #f #f "No DB at hand. SQL-server disabled")
				  #f)))))
        (with-exception-catcher
          (lambda (e)
            (close-all db-close sql-server socket)
            (raise e))
          (lambda ()
            (if db-at-hand
              (init-db db-fold-left))
	    (if sql-server
	      (begin
		(debug-message "Start the SQL server")
		(thread-start! sql-server)))
	    (if (not (null? input-files))
	      (if db-fold-left
		(apply add-logs db-fold-left bulk-size follow
		                (and (number? round-data)
				     round-data)
				input-files)
		(raise "No DB or socket connection. Adding data isn't possible")))
	    (if round-data
	      (if db-at-hand
		(round-all-logs db-fold-left
				(and (number? round-data)
				     round-data))
		(raise "No DB at hand. Rounding isn't possible")))
	    (if report-format
	      (if db-fold-left
		(do-report db-fold-left report-format
			   sdate edate minsize maxsize
			   ident-pat uri-pat limit summary)
		(raise "No DB or socket connection. Reporting isn't possible")))
	    (if sql-server
	      (thread-join! sql-server))
	    (close-all db-close sql-server socket)))))))

(define (main db-name socket-path bulk-size follow sdate edate ident-pat
              uri-pat minsize maxsize limit round-data report-format
              summary debug background log-file . input-files)
  (set! *debug* debug)
  (let* ((log-port (and log-file
			(open-output-file `(path: ,log-file append: #t create: maybe))))
	 (stderr (current-error-port))
	 (close-log! (lambda ()
		       (if log-port
			 (begin
			   (display-message "*** Log finished")
			   (current-error-port stderr)
			   (close-port log-port))))))
    (with-exception-catcher
      (lambda (e)
	(report-exception e)
	(if (and *debug* (not (signal-exception? e)))
	  (continuation-capture
	   (lambda (c)
	     (display-continuation-backtrace c (current-error-port)))))
	(close-log!)
	(raise e))
      (lambda ()
	(if log-port
	  (begin
	    (current-error-port log-port)
	    (display-message "*** Log started")))
	(let* ((pid (and background
			 (detach (and (string? background) background))))
	       (close-pid (lambda ()
			    (if pid
			      (delete-pidfile background)))))
	  (with-exception-catcher
	    (lambda (e)
	      (close-pid)
	      (raise e))
	    (lambda ()
	      (apply do-main db-name socket-path bulk-size follow sdate edate
		     ident-pat uri-pat minsize maxsize limit round-data
		     report-format summary debug input-files)
	      (close-pid))))
	(close-log!)))))

(signal-set-exception! *SIGHUP*)
(signal-set-exception! *SIGTERM*)
(signal-set-exception! *SIGINT*)
(signal-set-exception! *SIGQUIT*)

(with-exception-catcher
  (lambda (e)
    (if (signal-exception? e)
      (exit 0)
      (exit 1)))
  (lambda ()
    (let ((args (with-exception-catcher
		  (lambda (e)
		    #f)
		  (lambda ()
		    (apply scan-args (cdr (command-line)))))))
      (if args
	(apply main args)
	(begin
	  (usage)
	  (exit 1))))))
