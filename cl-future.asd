(in-package :asdf)

(defpackage future
  (:use :cl)
  (:export #:terminate-children #:future #:get-future-value #:future-mapcar
	   #:*total-slaves* #:*current-slave* #:*spawn-child-hooks*))

(defvar *future-path* *load-truename*)

(defsystem cl-future 
  :depends-on (osicat cl-store)
  :components 
  ((:file "future"))
  :perform (asdf:load-op :after (op c)
			 (if (find-package 'swank)
			     (load (make-pathname :name "future-swank"
						  :type "lisp"
						  :defaults *future-path*)))
			 (if (find-package 'clsql)
			     (load (make-pathname :name "future-clsql"
						  :type "lisp"
						  :defaults *future-path*)))))

   
  
	    