
# # Entertainment_Booking_System
# event_details/event_type/manager
# event_booked
# customer/event_booked



# # ecommerce
# customer/orders
# orders/lineitem
# part/supplier
# supplier/part



# # Exam Question and Hardcoded values
# users/cart
# users/orders
# orders/lineitem
# products

tables=["users","orders","lineitem","shoppingCart","products","added"]
tNo={"users":0,"orders":1,"lineitem":2,"shoppingCart":3,"products":4,"added":5}
adj={"lineitem":["orders"],
    "orders":["users"],
    "users":["shoppingCart"],
    "shoppingCart":[],
    "added":["shoppingCart","products"],
    "products":[]
}
relations={
"shoppingCart":["users","added"],
"users":["orders"],
"orders":["lineitem"],
"products":["added"],
"lineitem":[],
"added":[]
}

