(in-package :asdf)

(defpackage future
  (:use :cl)
  (:export #:terminate-children #:future #:get-future-value #:future-mapcar
	   #:*total-slaves*))

(defvar *future-path* *load-truename*)

(defsystem cl-future 
  :depends-on (osicat cl-store)
  :components 
  ((:file "future"))
  :perform (asdf:load-op :after (op c)
			 (if (find-package 'swank)
			     (load (make-pathname :name "future-swank"
						  :type "lisp"
						  :defaults *future-path*)))))

   
  
	    