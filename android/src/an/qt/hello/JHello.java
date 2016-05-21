package an.qt.hello;
import java.lang.String;
public class JHello {

        private String bla;
        public JHello(){}
        public JHello(String blala) {
                bla=blala;
        }
        public String reHello(int i)
        {
                if(i==1)
                {
                        return "hello"+bla;
                }
                return "Byb"+bla;
        }

        public static int reintlou(int i)
        {
                return i;
        }
        public static String reintlou2(int i)
        {
                JHello.consoleblala("native方法main中");
                JHello.consoleblalaC("native方法Class中");
                JHello2.consoleblalaNewC("native方法NewClassz中");
                return "izaza";
        }

        public static native void consoleblala(String blala);
        public static native void consoleblalaC(String blala);
}
class JHello2{
    public static native void consoleblalaNewC(String blala);
}
