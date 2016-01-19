package com.tdt.aws.controller;

import com.tdt.aws.dao.BaseDao;
import com.tdt.aws.dao.UserDAO;
import com.tdt.aws.util.DynamoDBUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpSession;

/**
 * Created by khoi on 1/17/2016.
 */
@Controller
public class LoginController extends BaseController {

    @Autowired
    private HttpSession session;

    @Autowired
    private UserDAO userDAO;

    /**
     * reset tables
     */
    @RequestMapping("/reset")
    @ResponseBody
    public String resetTable() {

        DynamoDBUtils.deleteTable(BaseDao.USER_TABLE_NAME);
        DynamoDBUtils.createTable(BaseDao.USER_TABLE_NAME, 5, 5, BaseDao.USER_TABLE_PK_NAME, BaseDao.USER_TABLE_PK_TYPE);
        return "done";
    }

    /**
     * Rest api to create user
     *
     * @param email    email
     * @param password pwd
     * @return message
     */
    @RequestMapping(value = "/signup", method = RequestMethod.POST)
    @ResponseBody
    public String createUser(String email, String password) {

        userDAO.createUser(email, password);
        return "created";
    }

    @RequestMapping(value = "/login", method = RequestMethod.POST, produces = "text/plain")
    @ResponseBody
    public String signin(@RequestParam String email,
                         @RequestParam String password,
                         HttpServletResponse response) throws Exception {

        return userDAO.checkLogin(email, password) ? "login successfully!" : "login failed";
    }

    @Override
    protected HttpSession getSession() {
        return session;
    }
}
