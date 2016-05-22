#include "a.h"

A::A()
{
    qDebug()<<"insideClass"<<registerNativeMthode();//放在这就崩了
    qDebug()<<"insideNewClass"<<registerNativeMthode2();//没注册就用果然
    QAndroidJniObject stra=QAndroidJniObject::fromString("ririri");
    jint inttyp=1;
    QAndroidJniObject jh("an/qt/hello/JHello","(Ljava/lang/String;)V",stra.object<jstring>());//注意.object()
//    QAndroidJniObject jh("an/qt/hello/JHello","(Ljava/lang/String;)V",stra.object());//注意.object()暂时都可以
    QAndroidJniObject restr=jh.callObjectMethod("reHello","(I)Ljava/lang/String;",inttyp);
    jint staticf=QAndroidJniObject::callStaticMethod<jint>("an/qt/hello/JHello","reintlou","(I)I",inttyp);//注意<jint>  jint 匹配
    QAndroidJniObject staticf2=QAndroidJniObject::callStaticObjectMethod("an/qt/hello/JHello","reintlou2","(I)Ljava/lang/String;",inttyp);
    QAndroidJniObject path=QAndroidJniObject::callStaticObjectMethod("an/qt/hello/JHello","returnSdcardPath","()Ljava/lang/String;");//
//    jstring path2=QAndroidJniObject::callStaticMethod<jstring>("an/qt/hello/JHello","returnSdcardPath","()Ljava/lang/String;");
//    jstring path3=QAndroidJniObject::fromString("ASD");这也不行
//    jstring要用callStaticObjectMethod
//    注意返回值类型不能是基础类型
    qDebug()<<restr.toString();
    qDebug()<<staticf;
    qDebug()<<staticf2.toString();
    qDebug()<<"path:"+path.toString();
//    qDebug()<<stra;//直接打印不行
    qDebug()<<"Asd";
//    qDebug()<<"insideClass"<<registerNativeMthode();//放在这就崩了
//    qDebug()<<"insideNewClass"<<registerNativeMthode2();//没注册就用果然
}
