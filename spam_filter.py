#!/usr/bin/python
#-*-coding:utf-8-*-

"""包含主要的业务逻辑,根据已有的数据建立模型以及定义方法判断一条信息是垃圾信息还是正常信息"""

import codecs;
import os;
import jieba;
import time;
from os.path import join;
import logging;
from config import *;
import json;

#记录每一个词在spam和normal中的频数
spamdict={};
normaldict={};
#分别记录spam和normal中所有词词频的和
spamdictsize=0;
normaldictsize=0;

comments=set([u"！", u"（", u"）", u"，", u"。", u"、", u"(", u")", u",", u"!", u"^", u"[", u"]", u"{", u"}",u"-",u"*",u"ヾ",u"＠",u"▽",u"゜",u"ノ"]);
stopwords = set([u"的",u"了",u"我",u"你",u"是"]);

spamlinedict = {};

logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
                    datefmt='%a, %d %b %Y %H:%M:%S',
                    filename=LOG_DIR+PATH_SPLIT+"%s-log.txt"%time.strftime('%d-%H-%M',time.localtime(time.time()))
                    )

#滤掉信息中的无意义的特殊字符
def stripmsg(message):
    newmsg = message;
    for word in comments:
        newmsg = newmsg.replace(word,"");
    return newmsg;

#找出pattern在str中第num次出现的起始下标，num从1开始
def findn(str,pattern,num):
    i=str.find(pattern);
    l=len(pattern);
    while(i!=-1 and num>1):
        i=str.find(pattern,i+l);
        num-=1;
    return i;

#从一个原始记录文件中提取弹幕信息正文
#contidx表示信息的正文在csv记录文件中的列数
#正常记录文件中信息正文在csv文件的第5列
#垃圾信息记录文件中信息正文在csv文件的第7列
def extractmessage(fpath,contidx=5):
    logging.debug((u"从记录文件%s中提取信息"%fpath).encode(CODING));
    output=fpath.replace(DATA_DIR,RECORD_DIR).replace('.csv','.txt');
    f1=codecs.open(fpath,'r',CODING);
    f2=codecs.open(join(output),'w',CODING);
    for eachline in f1:
        posbeg=findn(eachline,',',contidx);
        posbeg+=1;
        if(eachline[posbeg]!='"'):
            continue;
        posbeg+=1;
        eachline=eachline[posbeg:];
        posend=eachline.find('"');
        if(posend==0):
            continue;
        f2.write(eachline[:posend]+"\n");
    f1.close();
    f2.close();

#对所有的原始记录文件进行提取操作
#正常记录文件中信息正文在csv文件的第5列
#垃圾信息记录文件中信息正文在csv文件的第7列
def extractmsg():    
    for fpath in os.listdir(NORMAL_DATA_DIR):
        fpath=join(NORMAL_DATA_DIR,fpath);
        extractmessage(fpath,5);
    for fpath in os.listdir(SPAM_DATA_DIR):
        fpath=join(SPAM_DATA_DIR,fpath);
        if(fpath[-3:]!='csv'):
            continue;
        extractmessage(fpath,7,False);
		
#对提取出的弹幕信息进行分词，统计其中出现的所有词的词频
def splitandstat(doclist,isnormal=True):
    #分别记录spam信息和normal信息一共分出词的个数,即所有词的词频的和
    global normaldictsize;
    global spamdictsize;
    
    #正常信息和垃圾信息的词频分别记录在不同的字典中,写入不同的对应文件中
    if isnormal:
        recdict=normaldict;
        output=codecs.open(join(DICT_DIR,'normaldict.txt'),'w',CODING);
        jsonfile=codecs.open(join(DICT_DIR,'normaldict.json'),'w',CODING);
    else:
        recdict=spamdict;
        output=codecs.open(join(DICT_DIR,'spamdict.txt'),'w',CODING);
        jsonfile=codecs.open(join(DICT_DIR,'spamdict.json'),'w',CODING);
    
    for doc in doclist:
        logging.debug((u"对记录 %s 中文件分词."%doc).encode(CODING));
        f=codecs.open(doc,'r',CODING);
        for eachline in f:
            eachline=eachline.strip();
            if isnormal==True:
                if spamlinedict.has_key(eachline):
                    continue;
            else:
                spamlinedict[eachline]=1;
            neachline=stripmsg(eachline);
            #wordlist=jieba.analyse.extract_tags(eachline)
            wordlist=list(jieba.cut(neachline,cut_all=False));
            logging.debug((u"'%s'分词结果:共%d个词项"%(eachline,len(wordlist))).encode(CODING)+":[ " + ",".join(wordlist).encode(CODING)+" ]");
            #所有词的词频默认为1，第一次出现时记为2，以后每次加一。分类时对未出现过的词认为其词频为1
            for word in wordlist:
                if word.strip()=="" or word in stopwords:
                    continue;
                if recdict.has_key(word):
                    incrnum=1;
                    recdict[word]=recdict[word]+incrnum;
                else:
                    incrnum=2;
                    recdict[word]=incrnum;
                
                if isnormal==True:
                    normaldictsize+=incrnum;
                else:
                    spamdictsize+=incrnum;
        f.close()
    json.dump(recdict,jsonfile,indent=4);
    for word,count in recdict.iteritems():
        output.write("%s %d\n"%(word,count));
    output.close();

#分类，返回是否是垃圾信息
#用于测试时可以传入是否是正常信息判断是否分类正确以记录错误日志
def classify(message,yita=1.31):
    global normaldictsize;
    global spamdictsize;
    if len(set(message))<len(message)/5:
        return (True,-1.0);
    #msglist=jieba.analyse.extract_tags(message)
    nmessage = stripmsg(message);
    msglist=list(jieba.cut(nmessage,cut_all=False));
    msgsize=len(msglist);
    logmessage=(u"'%s'分词结果：共%d个词项"%(message,msgsize))+u" [ " + u",".join(msglist)+u" ]";
    nspam=1;
    nnormal=1;
    normalsize=normaldictsize;
    spamsize=spamdictsize;
    rate=float(normalsize)/float(spamsize);
    for msgword in msglist:
        if msgword.strip()=="" or msgword in stopwords:
            msgsize-=1;
            continue;
        normalcount=normaldict.get(msgword,1);
        spamcount=spamdict.get(msgword,1);
        nnormal*=normalcount;
        nspam*=spamcount;
        logmessage += u"\n词项'%s' normal(%d) spam(%d)."%(msgword,normalcount,spamcount);
    if msgsize>=1:
        nspam=nspam*rate**(msgsize-1);
    ratio = nspam/float(nnormal);
    logmessage +=u"\n%s最终分类结果%f"%(message,ratio);
    if(ratio>yita):
        isspam=True;
    else:
        isspam=False;
    logging.debug(logmessage.encode(CODING));
    return (isspam,ratio);

#对一个文件中记录的所有弹幕信息进行分类测试
def dotest(testfile,normalinfact=True):
    logging.debug("Testing " + testfile);
    errorlog.write("Testing %s\n"%testfile);
    print "Testing " + testfile;
    total=0;
    right=0;
    for eachline in codecs.open(testfile,'r',CODING):
        eachline = eachline.strip();
        if normalinfact==True:
            if spamlinedict.has_key(eachline):
                continue;
        total+=1;
        if classify(eachline)[0]==True:
            if normalinfact==False:
                right+=1;
            else:
                errorlog.write(eachline);
                errorlog.write(u"\n");
            logging.debug(u"判别为广告内容".encode(CODING));
        else:
            if normalinfact==True:
                right+=1;
            else:
                errorlog.write(eachline);
                errorlog.write(u"\n");
            logging.debug(u"判别为正常内容".encode(CODING));
    print "Right ratio %d/%d \n"%(right,total);
    logging.debug("Right ratio %d/%d \n"%(right,total));

def test():
    global errorlog;
    errorlog=codecs.open(LOG_DIR+PATH_SPLIT+"%s-errlog.txt"%time.strftime('%d-%H-%M'),'w',CODING);
    logging.debug("***************测试过程**********************");
   
    dotest(join(TEST_DIR,"normal.txt"));
    dotest(join(TEST_DIR,"spam.txt"),False);
    errorlog.close();

#如果之前运行过,可以直接读取保存在字典中的词频等信息，不用再读取每个文件来统计
def init_with_dict():
    global normaldictsize;
    global spamdictsize;
    normaldictsize=0;
    spamdictsize=0;
    normaldict.update(json.load(codecs.open(join(DICT_DIR,'normaldict.json'),'r',CODING)));
    spamdict.update(json.load(codecs.open(join(DICT_DIR,'spamdict.json'),'r',CODING)));
    spamlinedict.update(json.load(codecs.open(join(DICT_DIR,'spamlinedict.json'),'r',CODING)));
    
    for key,val in normaldict.iteritems():
        normaldictsize+=int(val);
    for key,val in spamdict.iteritems():
        spamdictsize+=int(val);
    logging.debug((u"从已保存记录获得得正常词条 %d 垃圾词条%d"%(normaldictsize,spamdictsize)).encode(CODING));

#执行所有的步骤,统计词频信息。
def init_without_dict():
    extractmsg();
    normalsize=int(os.popen('wc -l %s/*|tail -1'%NORMAL_DIR).read().strip().split(" ")[0]);
    spamsize=int(os.popen('wc -l %s/*|tail -1'%SPAM_DIR).read().strip().split(" ")[0]);
    logging.debug((u"已从原始数据提取正常信息 %d 垃圾信息%d"%(normalsize,spamsize)).encode(CODING));
    
    normalfilelist=[join(NORMAL_DIR,doc) for doc in os.listdir(NORMAL_DIR)];
    spamfilelist=[join(SPAM_DIR,doc) for doc in os.listdir(SPAM_DIR)];
    
    splitandstat(spamfilelist,False);
    splitandstat(normalfilelist);
    json.dump(spamlinedict,codecs.open(join(DICT_DIR,'spamlinedict.json'),'w',CODING),indent=4);
    logging.debug((u"分词获得正常词条 %d 垃圾词条%d"%(normaldictsize,spamdictsize)).encode(CODING));
    spamlinedictfile=codecs.open(join(DICT_DIR,'spamlinedict.txt'),'w',CODING);
    for word,count in spamlinedict.iteritems():
        spamlinedictfile.write("%s %d\n"%(word,count));
    spamlinedictfile.close();
    #更新dicts的时间戳，保证data未改变时dicts在ls --sort=time输出中排在data之前
    os.popen('touch dicts');

#检查上一次分词统计后数据是否更新过
#判断data目录和dict目录哪一个比较新    
def checkupdate():
    updatelist=os.popen('ls -l --sort=time').read();
    dataidx=updatelist.find('data');
    dictidx=updatelist.find('dicts');
    return dataidx<dictidx;
    
def init():
    jieba.load_userdict('split/dictadd.txt');
    if checkupdate():
        init_without_dict();
    else:
        init_with_dict();
    
if __name__=='__main__':
    init();
    test();
