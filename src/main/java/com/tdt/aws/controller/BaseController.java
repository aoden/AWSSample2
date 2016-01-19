package com.tdt.aws.controller;


import org.springframework.http.HttpHeaders;
import org.springframework.stereotype.Component;

import javax.servlet.http.HttpSession;
import java.util.Arrays;

@Component
public abstract class BaseController {

    protected HttpHeaders getHeaders(String userAgent) {

        HttpHeaders headers = new HttpHeaders();
        headers.put("authCode", Arrays.asList(new String[]{(String) getSession().getAttribute("authCode")}));
        headers.put("authToken", Arrays.asList(new String[]{(String) getSession().getAttribute("authToken")}));
        headers.put("userAgent", Arrays.asList(new String[]{userAgent != null ? userAgent : (String) getSession().getAttribute("userAgent")}));
        return headers;
    }

    protected abstract HttpSession getSession();
}
