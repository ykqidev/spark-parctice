package com.proj.spark;

import java.util.Random;

public class TestRandom {

    public static void main(String[] args) {
        Random random = new Random(10);

        for (int i = 0; i < 10; i++) {
            System.out.println(random.nextInt());
        }

        System.out.println("====================================");
        random = new Random(10);
        for (int i = 0; i < 10; i++) {
            System.out.println(random.nextInt());
        }
    }
}
