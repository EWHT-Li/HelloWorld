#ifndef A_H
#define A_H

#include <QObject>
#include <QtAndroidExtras/QAndroidJniObject>
#include <QDebug>
#include <QtAndroidExtras>

class A
{
public:
    A();
    static void consoleblalaC(JNIEnv *env,jobject thiz,jstring blala)
    {qDebug()<<"Class";
        qDebug()<<"Classkekeke"<<blala;
    }

    bool registerNativeMthode()
    {
        JNINativeMethod methods[]{
          {"consoleblalaC","(Ljava/lang/String;)V",(void*)consoleblalaC}
        };
        const char *classname=
                "an/qt/hello/JHello";
        jclass clazz;
        QAndroidJniEnvironment env;
        QAndroidJniObject javaClass(classname);
        clazz=env->GetObjectClass(javaClass.object<jobject>());


    //    clazz=env->FindClass(classname);会使之无法启动
        bool result=false;
        if(clazz)
        {
            jint ret=env->RegisterNatives(clazz,methods,
                                          sizeof(methods)/sizeof(methods[0]));
            env->DeleteLocalRef(clazz);
            result=(ret>=0);
        }
        if(env->ExceptionCheck()) env->ExceptionClear();
        return result;
    }

    static void consoleblalaNewC(JNIEnv *env,jobject thiz,jstring blala)
    {qDebug()<<"NewClass";
        qDebug()<<"NewClasskekeke"<<blala;
    }

    bool registerNativeMthode2()
    {
        JNINativeMethod methods[]{
          {"consoleblalaNewC","(Ljava/lang/String;)V",(void*)consoleblalaNewC}
        };
        const char *classname=
                "an/qt/hello/JHello2";
        jclass clazz;
        QAndroidJniEnvironment env;
        QAndroidJniObject javaClass(classname);
        clazz=env->GetObjectClass(javaClass.object<jobject>());


    //    clazz=env->FindClass(classname);会使之无法启动
        bool result=false;
        if(clazz)
        {
            jint ret=env->RegisterNatives(clazz,methods,
                                          sizeof(methods)/sizeof(methods[0]));
            env->DeleteLocalRef(clazz);
            result=(ret>=0);
        }
        if(env->ExceptionCheck()) env->ExceptionClear();
        return result;
    }
};

#endif // A_H
