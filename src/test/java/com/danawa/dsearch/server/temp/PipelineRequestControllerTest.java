//package com.danawa.dsearch.server;
//
//import org.assertj.core.api.Assertions;
//import org.junit.After;
//import org.junit.Before;
//import org.junit.Test;
//import org.junit.runner.RunWith;
//import org.springframework.beans.factory.annotation.Autowired;
//import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
//import org.springframework.boot.test.context.SpringBootTest;
//import org.springframework.boot.test.web.client.TestRestTemplate;
//import org.springframework.boot.web.server.LocalServerPort;
//import org.springframework.http.HttpStatus;
//import org.springframework.http.MediaType;
//import org.springframework.http.ResponseEntity;
//import org.springframework.mock.web.MockHttpSession;
//import org.springframework.test.context.junit4.SpringRunner;
//import org.springframework.test.web.servlet.MockMvc;
//import org.springframework.test.web.servlet.MvcResult;
//import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;
//import org.springframework.test.web.servlet.result.MockMvcResultHandlers;
//import org.springframework.test.web.servlet.result.MockMvcResultMatchers;
//import org.springframework.test.web.servlet.setup.MockMvcBuilders;
//import org.springframework.web.context.WebApplicationContext;
//
//import javax.servlet.http.Cookie;
//import java.util.UUID;
//
////
////@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.MOCK)
////@RunWith(SpringRunner.class)
////@AutoConfigureMockMvc
////public class PipelineControllerTest {
////    @Autowired
////    WebApplicationContext context;
////
////    protected MockHttpSession session;
////
////    private MockMvc mockMvc;
////
////    @Before
////    public void setUp() throws Exception{
////        session = new MockHttpSession();
////    }
////
////    @After
////    public void clean(){
////        session.clearAttributes();
////    }
////
//////    @Test
//////    public void pipelineList() throws Exception{
//////        this.mockMvc = MockMvcBuilders.webAppContextSetup(context).build();
//////        String uuid = "104e674a-8297-4668-80b1-b540404a7e14";
//////        String url = "/auth/sign-in";
//////        String json = "{\"password\":\"admin\", \"email\":\"admin@example.com\", \"username\":\"admin\"}";
//////        String contentAsString = mockMvc.perform(MockMvcRequestBuilders.post(url).contentType(MediaType.APPLICATION_JSON).content(json))
//////                .andExpect(MockMvcResultMatchers.status().isOk())
//////                .andExpect(MockMvcResultMatchers.content().contentType(MediaType.APPLICATION_JSON))
//////                .andDo(MockMvcResultHandlers.print()).andReturn().getResponse().getContentAsString();
//////
//////        Cookie cookie = new Cookie("SET_DSEARCH_AUTH_USER", contentAsString);
//////        String url2 = "/pipeline/list";
//////        mockMvc.perform(MockMvcRequestBuilders.get(url2).cookie(cookie).header("cluster-id", UUID.fromString(uuid)))
//////                .andExpect(MockMvcResultMatchers.status().isOk());
//////    }
//////
//////    @Test
//////    public void getPipeline() throws Exception{
//////        this.mockMvc = MockMvcBuilders.webAppContextSetup(context).build();
//////        String uuid = "104e674a-8297-4668-80b1-b540404a7e14";
//////        String url = "/auth/sign-in";
//////        String json = "{\"password\":\"admin\", \"email\":\"admin@example.com\", \"username\":\"admin\"}";
//////        String contentAsString = mockMvc.perform(MockMvcRequestBuilders.post(url).contentType(MediaType.APPLICATION_JSON).content(json))
//////                .andExpect(MockMvcResultMatchers.status().isOk())
//////                .andExpect(MockMvcResultMatchers.content().contentType(MediaType.APPLICATION_JSON))
//////                .andDo(MockMvcResultHandlers.print()).andReturn().getResponse().getContentAsString();
//////
//////        Cookie cookie = new Cookie("SET_DSEARCH_AUTH_USER", contentAsString);
//////        String url2 = "/pipeline/test";
//////        mockMvc.perform(MockMvcRequestBuilders.get(url2).cookie(cookie).header("cluster-id", UUID.fromString(uuid)))
//////                .andExpect(MockMvcResultMatchers.status().isOk());
//////    }
////}
////
