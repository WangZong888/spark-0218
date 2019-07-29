/**
 * @program: spark-0218
 * @description:
 * @author: Mr.Wang
 * @create: 2019-07-04 10:49
 **/
public class FinalTest {

    public static final int name=16 ;
    public static final int age;
    static{
        age=12;
        System.out.println("赋值后不能再改变了");
       //name=12;
    }
}
