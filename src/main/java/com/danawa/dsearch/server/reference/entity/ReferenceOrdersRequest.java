package com.danawa.dsearch.server.reference.entity;

import java.io.Serializable;
import java.util.List;

public class ReferenceOrdersRequest implements Serializable {
    private List<Order> orders;

    public List<Order> getOrders() {
        return orders;
    }

    public void setOrders(List<Order> orders) {
        this.orders = orders;
    }

    public static class Order {
        private String id;
        private Integer order;

        public Order(String id, Integer order) {
            this.id = id;
            this.order = order;
        }

        public String getId() {
            return id;
        }

        public void setId(String id) {
            this.id = id;
        }

        public Integer getOrder() {
            return order;
        }

        public void setOrder(Integer order) {
            this.order = order;
        }
    }
}
