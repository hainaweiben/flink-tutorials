package com.flink.tutorials.java.chapter2_basics.generics;

public class MyArrayList<T> {

    private final int size;
    T[] elements;

    public MyArrayList(int capacity) {
        this.size = capacity;
        this.elements = (T[]) new Object[capacity];
    }

    public void set(T element, int position) {
        elements[position] = element;
    }

    @Override
    public String toString() {
        StringBuilder result = new StringBuilder();
        for (int i = 0; i < size; i++) {
            result.append(elements[i].toString());
        }
        return result.toString();
    }

    public <E> void printInfo(E element) {
        System.out.println(element.toString());
    }

    public static void main(String[] args){

        MyArrayList<String> strList = new MyArrayList<>(2);
        strList.set("first", 0);
        strList.set("second", 1);

        MyArrayList<Integer> intList = new MyArrayList<>(2);
        intList.set(11, 0);
        intList.set(22, 1);

        System.out.println(strList);
        System.out.println(intList);

        intList.printInfo("function");
    }

}
