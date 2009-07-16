(in-package :future)

;; if we are running interactively with swank, make sure that we close
;; the swank connection in the children, or the child will screw up
;; communication between swank and the parent
#+allegro
(progn
  (defun close-swank-connections ()
    (mapc #'(lambda (c)
	      (swank::close-connection c nil nil))
	  swank::*connections*)
    (setq swank::*connections* nil)
    )

  (pushnew 'close-swank-connections *spawn-child-hooks*))
