package com.danawa.fastcatx.server;

import com.danawa.fastcatx.server.entity.Academy;
import com.danawa.fastcatx.server.repository.AcademyRepository;
import com.danawa.fastcatx.server.repository.AcademyRepositorySupport;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;

import java.util.List;

//@SpringBootTest
//@ActiveProfiles("local")
//public class H2queryDslTest {
//
//    @Autowired
//    private AcademyRepository academyRepository;
//
//    @Autowired
//    private AcademyRepositorySupport academyRepositorySupport;
//
//    @Test
//    public void findQueryDsl() {
//        //given
//        String name = "admin";
//        String address = "admin@gmail.com";
//        academyRepository.save(new Academy(name, address));
//
//        //when
//        List<Academy> result = academyRepositorySupport.findByName(name);
//
//        //then
//        System.out.println(result);
//    }
//
//
//}
