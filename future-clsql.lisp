(in-package :future)

;; If we are using clsql, make sure to close the connections in the child lest the server get confused.
(defun close-clsql-connections ()
  (clsql:disconnect)
  )

(pushnew #'close-clsql-connections *spawn-child-hooks*)
