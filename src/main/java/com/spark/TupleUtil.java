package com.spark;

import com.tuple.ThreeTuple;
import com.tuple.TwoTuple;

import java.util.ArrayList;
import java.util.List;

/**
 *
 */
public class TupleUtil {
    public static <A, B> TwoTuple<A, B> tuple(A a, B b) {
        return new TwoTuple<A, B>(a, b);
    }

    public static <A, B, C> ThreeTuple<A, B, C> tuple(A a, B b, C c) {
        return new ThreeTuple<A, B, C>(a, b, c);
    }

    // 测试
    public static void main(String[] args) {
        List<Person> personList = new ArrayList<Person>();
        for(int i = 1; i < 26; i++) {
            Person person = new Person();
            person.setAge(i);
            person.setName("zengsong"+i);
            personList.add(person);
        }
        Integer totalProperty = 47;
//      TupleUtil<List<GoodsBean>, Integer> tupleUtil = new TupleUtil<List<GoodsBean>, Integer>(goodsBeans, totalProperty);
        TwoTuple<List<Person>, Integer> twoTuple = TupleUtil.tuple(personList, totalProperty);
        List<Person> list = twoTuple.first;
        for(Person person:list) {
            System.out.println(person.getName()+":"+person.getAge());
        }
        System.out.println(twoTuple.second);
    }
}
