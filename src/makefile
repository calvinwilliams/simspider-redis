# 此文件由makeobj.sh自动生成
############################################################
# 项目名 : 
# 模块名 : 
# 备  注 : 
############################################################

###### 源文件配置区
#@ c_FILE
c_FILE		=	\
			libsimspider-redis.c \

c_FILE2		=	\
			simspider-redis.c \

###### 目标文件、安装目录配置区
LIB		=	libsimspider-redis.so
LIBINST		=	$(HOME)/exlib
BIN		=	simspider-redis
BININST		=	$(HOME)/exbin
HDER		=	libsimspider-redis.h
HDERINST	=	$(HOME)/exinc/simspider-redis

###### 编译选项
CFLAGS		=	-I. \
			-I$(HOME)/include \
			-I/usr/include \
			-I/usr/local/include/hiredis \
			-I$(HOME)/exinc/simspider \

###### 链接选项
LFLAGS		=	-L. \
			-L$(HOME)/exlib \
			-L$(HOME)/lib \
			-lsimspider \
			-L/usr/lib \
			-lcurl \
			-L/usr/local/lib \
			-lhiredis \

###### 额外宏定义区
CLEAN_ADDITION	=	simspider-redis.log

###### 加载mktpl模板库
#@ make_all
#@ make_clean
#@ make_install
#@ make_install_BININST
#@ make_install_LIBINST
#@ make_install_HDERINST

include $(MKTPLDIR)/makeobj_$(MKTPLOS).inc

###### 目标文件依赖关系
libsimspider-redis.so		:	$(c_FILE_o)
	$(CC) $(SOFLAGS) -o $@ $(c_FILE_o) $(LFLAGS)

simspider-redis		:	$(c_FILE2_o)
	$(CC) $(ESOFLAGS) -o $@ $(c_FILE2_o) $(LFLAGS) -lsimspider-redis

