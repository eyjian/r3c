r3c基于redis官方的c库hiredis实现，全称是redis cluster C++ client，支持redis cluster。

编译链接r3c时，默认认为hiredis的安装目录为/usr/local/hiredis，
但可以在执行make时指定hiredis安装目录，如假设hiredis安装目录为/tmp/hiredis：make HIREDIS=/tmp/hiredis，
或修改Makefile中变量HIREDIS的值来指定hiredis实现的安装目录。

编译r3c成功后，将生成libr3c.a静态库，没有共享库被生成。
也可以直接将r3c.h和r3c.cpp两个文件加入到自己项目代码中一起编译，而不独立编译r3c。

r3c_cmd.cpp是r3c的命令行测试工具，具备redis-cli的一些功能，但用法有些不同，将逐步将覆盖redis-cli的所有功能。
