(in-package :future)

(defparameter *is-slave* nil
  "Are we currently running in a slave process?")

(defvar *total-slaves* 3
  "The maximum number of slaves to run at any one time.")

(defvar *free-slaves* (loop for i from 1 to *total-slaves* collect i))

(defvar *current-slave* 0
  "if we are running on a slave, the slave number (from 1 to
  *total-slaves*), 0 for the controller. This is useful for making
  sure that processes don't collide on shared resources (like ports,
  for example). Only one process will be running using a given
  *current-slave* number at any time.")

(defvar *futures-awaiting-status* (make-hash-table :test #'eql)
  "This table contains those futures that we haven't noticed have
terminated yet.  When wait comes around and reaps them, we remove them
from the table" )

(defvar *spawn-child-hooks* nil
  "List of functions that are executed in a newly spawned child before
  anything else is done")

(defvar *before-spawn-hooks* nil
  "List of functions that are executed in the parent before forking
  the child")

(defun fork ()
  "We need to handle fork differently in different lisps or else they get really confused"
  #+sbcl
  (return-from fork (sb-posix:fork))
  #+allegro
  (return-from fork (excl.osi:fork))
  (nix:fork))
  
(defclass future ()
    ((pid :initarg :pid
	  :reader pid
	  :documentation "Process number that is evaluating the future. Null if evaluation is happening in process")
     (result :initform 'unbound
	     :documentation "The result returned by the child process")
     (code :reader code
	   :initform nil
	   :documentation "The exit code for the subprocess. Null if process has not been reaped")
     (slave :initarg :slave
            :documentation "The number (1 to *total-slaves*) of the
            slave this future is currently running on. Zero if on the
            controller.")))

(defmethod initialize-instance :after ((f future) &key &allow-other-keys)
  (with-slots (pid) f
    (cond ((not (null pid))
	   (setf (gethash pid *futures-awaiting-status*)
		 f)))))

(defmethod read-result ((f future) status-code)
  (with-slots (pid result code slave) f
    (setf code status-code)
    (let* ((path (format nil "/tmp/pid.~d" pid)))
      (destructuring-bind (output stored-result)
	  (cl-store:restore path)
	(princ output)
	(setf result stored-result)
	(delete-file path)
	(remhash pid *futures-awaiting-status*)
        (push slave *free-slaves*)))
    (cond ((not (eql code 0))
	   (error "Future terminated with error ~a" result)))))

(defun wait-for-slave ()
  "Wait for slaves to terminate until we are able to launch a new one"
  (loop while (>= (hash-table-count *futures-awaiting-status*) *total-slaves*)
       do
       (wait-for-one-slave)))

(defun wait-for-one-slave ()
  (multiple-value-bind (result status)
      (nix:waitpid 0)
    (cond ((> result 0)
	   (read-result (gethash result *futures-awaiting-status*) status))
	  (t
	   (error "No child processes, but *futures-awaiting-status* is not empty")))))
      
	
(defun terminate-children ()
  "Kill all currently running children."
  (maphash #'(lambda (key value)
               (declare (ignore key))
	       (with-slots (pid result code) value
		 (ignore-errors (nix:kill (pid value) 9))
		 (setf result (make-condition 'simple-error :format-control "Future killed by terminate-children")
		       code 1)))
	   *futures-awaiting-status*)
  ;; reap them so they don't confuse us later on 
  (loop while (> (nix:waitpid 0) 0))
  (setq *free-slaves* (loop for i from 1 to *total-slaves* collect i))
  (clrhash *futures-awaiting-status*))

(defun execute-future (fn)
  (cond (*is-slave*
	 ;; if we have already forked off, don't fork again.
	 ;; the parent needs to limit the forking behaviour 
	 (return-from execute-future 
	   (make-instance 'future :pid nil :result (funcall fn)))))
  (wait-for-slave)
  (mapc #'funcall *before-spawn-hooks*)
  (let ((this-slave (pop *free-slaves*))
        (pid (fork)))
    (cond ((eql pid 0)
	   (let* ((in  (make-string-input-stream ""))
		  (out (make-string-output-stream))
		  (tw (make-two-way-stream in out))
 		  (*standard-input* in)
 		  (*standard-output* out)
 		  (*error-output* out)
 		  (*trace-output* out)
 		  (*terminal-io* tw)
 		  (*debug-io* tw)
 		  (*query-io* tw)
                  (*current-slave* this-slave)
		  ) 

	     (let* ((output-pathname (format nil "/tmp/pid.~d" (nix:getpid)))
		    (*is-slave* t))

	       (handler-case 
		   (let ((result (progn 
                                   ;; we are the child - evaluate the expression and write it to disk
                                   (mapc #'funcall *spawn-child-hooks*)
                                   (funcall fn))))
		     (cl-store:store (list (get-output-stream-string out) result)
				     output-pathname)
                     (nix:exit 0)

		     (close tw) (close in) (close out)
		     (nix:exit 0))
		 (error (e)
		   (cl-store:store (list (get-output-stream-string out) e)
				   output-pathname)
		   (close tw) (close in) (close out)
		   (nix:exit 1))))))
	  (t
	   (setf (gethash pid *futures-awaiting-status*)
		 (make-instance 'future :pid pid :slave this-slave))))))
  
(defmacro future (&body body)
  "Evaluate expr in parallel using a forked child process. Returns a
'future' object whose value can be retrieved using
get-future-value. No side-effects made in <expr> will be visible from
the calling process."
  `(execute-future #'(lambda () ,@body)))

(defun get-future-value (expr &key (clean-up t))
"walk the list structure 'expr', replacing any futures with their
evaluated values. Blocks if a future is still running."
  (cond ((null expr) nil)
	((listp expr)
	 (cons (get-future-value (car expr) :clean-up clean-up)
	       (get-future-value (cdr expr) :clean-up clean-up)))
	((eq (class-name (class-of expr)) 'future)
	 (with-slots (pid result code) expr
	   (loop while (eq result 'unbound)
	      do (wait-for-one-slave))
	   (return-from get-future-value result)))
	(t
	 expr)))
	
(defun future-mapcar (fn list)
  (cond ((< (length list) (* *total-slaves* 3.0))
         (get-future-value (mapcar #'(lambda (x) (future (funcall fn x))) list)))
        (t
         ;; list is big enough that we can split it into big chunks to minimize # of forks required
         (let* ((chunk-size (/ (length list) (* *total-slaves* 3.0)))
                (cur-chunk nil)
                (chunks nil))
           ;; split the input list into chunks of size "chunk-size"
           ;; (or as close as possiblt)
           ;; note that because we are pushing, everything is reversed
           (loop for e in list
              with cur-count = 0.0
              do
              (cond ((< cur-count chunk-size)
                     (push e cur-chunk)
                     (incf cur-count)))
              (cond ((>= cur-count chunk-size)
                     (jp:pvalues cur-count chunk-size)
                     (push cur-chunk chunks)
                     (setq cur-chunk nil
                           cur-count (- cur-count chunk-size)))))
           (cond (cur-chunk
                  (push cur-chunk chunks)))
           (jp:pvalues chunk-size chunks)

           ;;evaluate the chunks in child processes
           (let ((results (get-future-value 
                           (mapcar #'(lambda (sublist)
                                       (future (mapcar fn sublist)))
                                   chunks)))
                 (sorted-results nil))
             ;; reverse things again so they come back in the correct order
             (loop for result in results do
                  (loop for sublist-result in result do
                       (push sublist-result sorted-results)))
             sorted-results)))))
