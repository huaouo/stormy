package com.huaouo.stormy.example.model;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class UserInfo {
    private int id;
    private String name;
    private String email;
    private String address;
}
