# 此文件由makeobj.sh自动生成
############################################################
# 项目名 : 
# 模块名 : 
# 备  注 : 
############################################################

###### 源文件配置区
#@ c_FILE
c_FILE2		=	\
			simspider-cluster.c \

###### 目标文件、安装目录配置区
BIN		=	simspider-cluster

###### 编译选项
CFLAGS		=	-I. \
			-I$(HOME)/include \
			-I/usr/include \
			-I/usr/local/include/hiredis \
			-I$(HOME)/exinc/simspider \
			-I$(HOME)/exinc/simspider-redis \

###### 链接选项
LFLAGS		=	-L. \
			-L$(HOME)/exlib \
			-L$(HOME)/lib \
			-lsimspider \
			-lsimspider-redis \
			-L/usr/lib \
			-lcurl \
			-L/usr/local/lib \
			-lhiredis \

###### 额外宏定义区
CLEAN_ADDITION	=	simspider-cluster*.log

###### 加载mktpl模板库
#@ make_all
#@ make_clean
#@ make_install
#@ make_install_BININST
#@ make_install_DFTHDERINST

include $(MKTPLDIR)/makeobj_$(MKTPLOS).inc

###### 目标文件依赖关系
simspider-cluster		:	$(c_FILE2_o)
	$(CC) $(ESOFLAGS) -o $@ $(c_FILE2_o) $(LFLAGS)

