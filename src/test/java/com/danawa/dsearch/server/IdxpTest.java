package com.danawa.dsearch.server;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.mock.web.MockHttpSession;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;
import org.springframework.test.web.servlet.result.MockMvcResultHandlers;
import org.springframework.test.web.servlet.result.MockMvcResultMatchers;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.springframework.web.context.WebApplicationContext;
import reactor.netty.http.Cookies;

import javax.servlet.http.Cookie;


@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.MOCK)
@RunWith(SpringRunner.class)
@AutoConfigureMockMvc
public class IdxpTest {

    @Autowired
    WebApplicationContext context;

    protected MockHttpSession session;

    private MockMvc mockMvc;

    @Before
    public void setUp() throws Exception{
        session = new MockHttpSession();
    }

    @After
    public void clean(){
        session.clearAttributes();
    }

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
    public void idxpLoginAfterTest()throws Exception{
        this.mockMvc = MockMvcBuilders.webAppContextSetup(context).build();
        String url = "/auth/sign-in";
        String json = "{\"password\":\"admin\", \"email\":\"admin@example.com\", \"username\":\"admin\"}";
        String contentAsString = mockMvc.perform(MockMvcRequestBuilders.post(url).contentType(MediaType.APPLICATION_JSON).content(json))
                .andExpect(MockMvcResultMatchers.status().isOk())
                .andExpect(MockMvcResultMatchers.content().contentType(MediaType.APPLICATION_JSON))
                .andDo(MockMvcResultHandlers.print()).andReturn().getResponse().getContentAsString();

        Cookie cookie = new Cookie("SET_DSEARCH_AUTH_USER", contentAsString);
        String url2 = "/collections/idxp/status?host=localhost&port=9200&collectionName=ackeyword";
        mockMvc.perform(MockMvcRequestBuilders.get(url2).cookie(cookie))
                .andExpect(MockMvcResultMatchers.status().isOk())
                .andExpect(MockMvcResultMatchers.content().contentType(MediaType.APPLICATION_JSON))
                .andDo(MockMvcResultHandlers.print());

        String url3 = "/collections/idxp?host=localhost&port=9200&collectionName=ackeyword&action=all";
        mockMvc.perform(MockMvcRequestBuilders.get(url3).cookie(cookie))
                .andExpect(MockMvcResultMatchers.status().isOk())
                .andExpect(MockMvcResultMatchers.content().contentType(MediaType.APPLICATION_JSON))
                .andDo(MockMvcResultHandlers.print());

        String url4 = "/collections/idxp/status?host=localhost&port=9200&collectionName=ackeyword";
        mockMvc.perform(MockMvcRequestBuilders.get(url2).cookie(cookie))
                .andExpect(MockMvcResultMatchers.status().isOk())
                .andExpect(MockMvcResultMatchers.content().contentType(MediaType.APPLICATION_JSON))
                .andDo(MockMvcResultHandlers.print());
    }
}
