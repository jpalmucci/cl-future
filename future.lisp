(in-package :future)

(defparameter *is-slave* nil
  "Are we currently running in a slave process?")

(defvar *total-slaves* 3
  "The max number of running slave processes")

(defvar *futures-awaiting-status* (make-hash-table :test #'eql)
  "This table contains those futures that we haven't noticed have terminated yet.
When wait comes around and reaps them, we remove them from the table"
)

(defvar *spawn-child-hooks* nil
  "List of functions that are executed in a newly spawned child before anything else is done")

(defclass future ()
    ((pid :initarg :pid
	  :reader pid
	  :documentation "Process number that is evaluating the future. Null if evaluation is happening in process")
     (result :initform 'unbound
	     :documentation "The result returned by the child process")
     (code :reader code
	   :initform nil
	   :documentation "The exit code for the subprocess. Null if process has not been reaped")))

(defmethod initialize-instance :after ((f future) &key &allow-other-keys)
  (with-slots (pid) f
    (cond ((not (null pid))
	   (setf (gethash pid *futures-awaiting-status*)
		 f)))))

(defmethod read-result ((f future) status-code)
  (with-slots (pid result code) f
    (setf code status-code)
    (let* ((path (format nil "/tmp/pid.~d" pid)))
      (destructuring-bind (output stored-result)
	  (cl-store:restore path)
	(princ output)
	(setf result stored-result)
	(delete-file path)
	(remhash pid *futures-awaiting-status*)))
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
  "Kill all currently runnint children"
  (maphash #'(lambda (key value)
	       (with-slots (pid result code) value
		 (ignore-errors (nix:kill (pid value) 9))
		 (setf result (make-instance 'error :format-control "Future killed by terminate-children")
		       code 1)))
	   *futures-awaiting-status*)
  ;; reap them so they don't confuse us later on 
  (loop while (> (nix:waitpid 0) 0))
  (clrhash *futures-awaiting-status*))

(defun execute-future (fn)
  (cond (*is-slave*
	 ;; if we have already forked off, don't fork again.
	 ;; the parent needs to limit the forking behaviour 
	 (return-from execute-future 
	   (make-instance 'future :pid nil :result (funcall fn)))))
  (wait-for-slave)
  (let ((pid (excl.osi:fork)))
    (cond ((eql pid 0)
	   ;; we are the child - evaluate the expression and write it to disk
	   (mapc #'funcall *spawn-child-hooks*)
	   (let* ((in (make-string-input-stream ""))
		  (out (make-string-output-stream))
		  (tw (make-two-way-stream in out))
		  (output-pathname (format nil "/tmp/pid.~d" (nix:getpid)))
		  (*is-slave* t)
		  (*standard-input* in)
		  (*standard-output* out)
		  (*error-output* out)
		  (*trace-output* out)
		  (*terminal-io* tw)
		  (*debug-io* tw)
		  (*query-io* tw)
		  )
	     (handler-case 
		 (let ((result (funcall fn)))
		   (cl-store:store (list (get-output-stream-string out) result)
				   output-pathname)
		   (close tw) (close in) (close out)
		   (nix:exit 0))
	       (error (e)
		 (cl-store:store (list (get-output-stream-string out) e)
				 output-pathname)
		   (close tw) (close in) (close out)
		   (nix:exit 1)))))
	  (t
	   (setf (gethash pid *futures-awaiting-status*)
		 (make-instance 'future :pid pid))))))
  
(defmacro future (&body body)
  `(execute-future #'(lambda () ,@body)))

(defun get-future-value (f &key (clean-up t))
  (cond ((null f) nil)
	((listp f)
	 (cons (get-future-value (car f) :clean-up clean-up)
	       (get-future-value (cdr f) :clean-up clean-up)))
	((eq (class-name (class-of f)) 'future)
	 (with-slots (pid result code) f
	   (loop while (eq result 'unbound)
	      do (wait-for-one-slave))
	   (return-from get-future-value result)))
	(t
	 f)))
	
(defun future-mapcar (fn list &key (chunk-size 1))
  (let ((futures (mapcar #'(lambda (x)
			     (future (funcall fn x)))
			 list)))
    (mapcar #'get-future-value futures)))

