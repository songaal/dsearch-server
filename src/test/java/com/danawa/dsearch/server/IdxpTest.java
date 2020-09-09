package com.danawa.dsearch.server;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;
import org.springframework.test.web.servlet.result.MockMvcResultHandlers;
import org.springframework.test.web.servlet.result.MockMvcResultMatchers;


@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.MOCK)
@RunWith(SpringRunner.class)
@AutoConfigureMockMvc
public class IdxpTest {

    @Autowired
    MockMvc mockMvc;

    @Test
    public void idxpLoginTest() throws Exception {
        String url = "/auth/sign-in";
        String json = "{\"password\":\"admin\", \"email\":\"admin@example.com\", \"username\":\"admin\"}";
        mockMvc.perform(MockMvcRequestBuilders.post(url).contentType(MediaType.APPLICATION_JSON).content(json))
                .andExpect(MockMvcResultMatchers.status().isOk())
                .andExpect(MockMvcResultMatchers.content().contentType(MediaType.APPLICATION_JSON))
                .andDo(MockMvcResultHandlers.print());
    }

    @Test
    public void idxpIndexingTest() throws Exception {
        String url = "/auth/sign-in";
        String json = "{\"password\":\"admin\", \"email\":\"admin@example.com\", \"username\":\"admin\"}";
        mockMvc.perform(MockMvcRequestBuilders.post(url).contentType(MediaType.APPLICATION_JSON).content(json))
                .andExpect(MockMvcResultMatchers.status().isOk())
                .andExpect(MockMvcResultMatchers.content().contentType(MediaType.APPLICATION_JSON))
                .andDo(MockMvcResultHandlers.print());

        String url2 = "/collections/idxp/status?esHost=localhost&esPort=9200&esCollectionName=ackeyword";
        mockMvc.perform(MockMvcRequestBuilders.get(url2))
                .andExpect(MockMvcResultMatchers.status().isOk())
                .andExpect(MockMvcResultMatchers.content().contentType(MediaType.APPLICATION_JSON))
                .andDo(MockMvcResultHandlers.print());

        String url3 = "/collections/idxp?esHost=localhost&esPort=9200&esCollectionName=ackeyword&action=all";
        mockMvc.perform(MockMvcRequestBuilders.get(url3))
                .andExpect(MockMvcResultMatchers.status().isOk())
                .andExpect(MockMvcResultMatchers.content().contentType(MediaType.APPLICATION_JSON))
                .andDo(MockMvcResultHandlers.print());

        String url4 = "/collections/idxp/status?esHost=localhost&esPort=9200&esCollectionName=ackeyword";
        mockMvc.perform(MockMvcRequestBuilders.get(url4))
                .andExpect(MockMvcResultMatchers.status().isOk())
                .andExpect(MockMvcResultMatchers.content().contentType(MediaType.APPLICATION_JSON))
                .andDo(MockMvcResultHandlers.print());
    }
}
