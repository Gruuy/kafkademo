package com.gruuy.kafka;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

//@SpringBootTest
class KafkademoApplicationTests {

    @Test
    void contextLoads() {
        int[] nums = {2, 7, 11, 15};int target = 9;
        Map<Integer,Integer> hashMap=new HashMap<>();
        hashMap.put(nums[0],0);
        for(int i=1;i<nums.length;i++){
            if(hashMap.get(target-nums[i])!=null){
                int[] result=new int[2];
                result[0]=i;
                result[1]=hashMap.get(target-nums[i]);
                Arrays.stream(result).forEach(a-> System.out.println(a ));

                return;
            }else {
                hashMap.put(nums[i],i);
            }
        }
    }

}
