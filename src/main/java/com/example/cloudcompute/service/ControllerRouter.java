package com.example.cloudcompute.service;

import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@Controller
public class ControllerRouter {
    @GetMapping("/index")
    public String getIndexPage()
    {
        return "index";
    }

    @GetMapping("/login")
    public String getLoginPage()
    {
        return "login";
    }

    @GetMapping("/register")
    public String getRegisterPage()
    {
        return "register";
    }
}
