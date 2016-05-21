#include <QApplication>
#include <QQmlApplicationEngine>
#include <QAndroidJniEnvironment>
#include <QAndroidJniObject>
#include <jni.h>
#include "a.h"

static void consoleblala(JNIEnv *env,jobject thiz,jstring blala)
{qDebug()<<"main";
    qDebug()<<"mainkekeke"<<blala;
}
//static 能要能不要，不要会警告
bool registerNativeMthode()
{
    JNINativeMethod methods[]{
      {"consoleblala","(Ljava/lang/String;)V",(void*)consoleblala}
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


int main(int argc, char *argv[])
{
    QApplication app(argc, argv);
    qDebug()<<registerNativeMthode();
    A asd;

    QQmlApplicationEngine engine;
    engine.load(QUrl(QStringLiteral("qrc:/main.qml")));

    return app.exec();
}
