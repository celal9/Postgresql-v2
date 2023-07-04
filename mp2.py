from seller import Seller

import psycopg2
import configparser
from messages import *

"""
    Splits given command string by spaces and trims each token.
    Returns token list.
"""


def tokenize_command(command):
    tokens = command.split(" ")
    return [t.strip() for t in tokens]


class Mp2Client:
    def __init__(self, config_filename):
        cfg = configparser.ConfigParser()
        cfg.read(config_filename)
        self.db_conn_params = cfg["postgresql"]
        self.conn = None

    """
        Connects to PostgreSQL database and returns connection object.
    """

    def connect(self):
        self.conn = psycopg2.connect(**self.db_conn_params)
        self.conn.autocommit = False

    """
        Disconnects from PostgreSQL database.
    """

    def disconnect(self):
        self.conn.close()

    """
        Prints list of available commands of the software.
    """

    def help(self):
        # prints the choices for commands and parameters
        print("\n*** Please enter one of the following commands ***")
        print("> help")
        print("> sign_up <seller_id> <subscriber_key> <zip_code> <city> <state> <plan_id>")
        print("> sign_in <seller_id> <subscriber_key>")
        print("> sign_out")
        print("> show_plans")
        print("> show_subscription")
        print("> change_stock <product_id> <add or remove> <amount>")
        print("> show_quota")
        print("> subscribe <plan_id>")
        print("> ship <product_id_1> <product_id_2> <product_id_3> ... <product_id_n>")
        print("> calc_gross")
        print("> show_cart <customer_id>")
        print("> change_cart <customer_id> <product_id> <seller_id> <add or remove> <amount>")
        print("> purchase_cart <customer_id>")
        print("> quit")

    """
        Saves seller with given details.
        - Return type is a tuple, 1st element is a boolean and 2nd element is the response message from messages.py.
        - If the operation is successful, commit changes and return tuple (True, CMD_EXECUTION_SUCCESS).
        - If any exception occurs; rollback, do nothing on the database and return tuple (False, CMD_EXECUTION_FAILED).
    """

    def sign_up(self, seller_id, sub_key, zip, city, state, plan_id):
        try:
            cursor = self.conn.cursor()
            cursor.execute("SELECT seller_id FROM sellers WHERE seller_id = %s;", (seller_id,))
            if cursor.fetchone() is not None:
                return False, CMD_EXECUTION_FAILED
            cursor.execute("""
                            INSERT INTO sellers (seller_id, seller_zip_code_prefix, seller_city, seller_state)
                            VALUES (%s, %s, %s, %s);
                            """,
                           (seller_id, zip, city, state))
            cursor.execute("""
                                        INSERT INTO seller_subscription (seller_id, subscriber_key, session_count, plan_id)
                                        VALUES (%s, %s, %s, %s);
                                        """,
                           (seller_id, sub_key, 0, plan_id))
            self.conn.commit()
            cursor.close()
            return True, CMD_EXECUTION_SUCCESS
        except Exception as e:
            return False, CMD_EXECUTION_FAILED

    """
        Retrieves seller information if seller_id and subscriber_key is correct an d seller's session_count < max_parallel_sessions.
        - Return type is a tuple, 1st element is a seller object and 2nd element is the response message from messages.py.
        - If seller_id or subscriber_key is wrong, return tuple (None, USER_SIGNIN_FAILED).
        - If session_count < max_parallel_sessions, commit changes (increment session_count) and return tuple (seller, CMD_EXECUTION_SUCCESS).
        - If session_count >= max_parallel_sessions, return tuple (None, USER_ALL_SESSIONS_ARE_USED).
        - If any exception occurs; rollback, do nothing on the database and return tuple (None, USER_SIGNIN_FAILED).
    """

    def sign_in(self, seller_id, sub_key):
        try:
            cursor = self.conn.cursor()
            cursor.execute("SELECT seller_id, subscriber_key, session_count, plan_id "
                           "FROM seller_subscription "
                           "WHERE seller_id = %s AND subscriber_key = %s;",
                           (seller_id, sub_key))
            queryCustomer = cursor.fetchone()
            if queryCustomer is None:
                return None, USER_SIGNIN_FAILED

            cursor.execute("SELECT max_parallel_sessions "
                           "FROM subscription_plans "
                           "WHERE plan_id = %s;",
                           (queryCustomer[3],))
            queryCustomerPlan = cursor.fetchone()

            if queryCustomer[2] >= queryCustomerPlan[0]:
                return None, USER_ALL_SESSIONS_ARE_USED
            else:
                cursor.execute("""
                                UPDATE seller_subscription
                                SET session_count = session_count + 1
                                WHERE seller_id = %s;
                                """,
                               (seller_id,))
                self.conn.commit()
                cursor.close()
                return Seller(queryCustomer[0], queryCustomer[2], queryCustomer[3]
                              ), CMD_EXECUTION_SUCCESS
        except Exception as e:
            return None, USER_SIGNIN_FAILED

    """
        Signs out from given seller's account.
        - Return type is a tuple, 1st element is a boolean and 2nd element is the response message from messages.py.
        - Decrement session_count of the seller in the database.
        - If the operation is successful, commit changes and return tuple (True, CMD_EXECUTION_SUCCESS).
        - If any exception occurs; rollback, do nothing on the database and return tuple (False, CMD_EXECUTION_FAILED).
    """

    def sign_out(self, seller):
        try:
            cursor = self.conn.cursor()
            cursor.execute("SELECT seller_id "
                           "FROM sellers "
                           "WHERE seller_id = %s;",
                           (seller.seller_id,))
            queryCustomer = cursor.fetchone()
            if queryCustomer is None or queryCustomer[0] == 0:
                # Fail if such a customer dne or his/her session count is 0 somehow
                return False, CMD_EXECUTION_FAILED

            cursor.execute("""
                            UPDATE seller_subscription
                            SET session_count = session_count - 1
                            WHERE seller_id = %s;
                            """,
                           (seller.seller_id,))
            self.conn.commit()
            cursor.close()
            return True, CMD_EXECUTION_SUCCESS
        except Exception as e:
            return False, CMD_EXECUTION_FAILED

    """
        Quits from program.
        - Return type is a tuple, 1st element is a boolean and 2nd element is the response message from messages.py.
        - Remember to sign authenticated user out first.
        - If the operation is successful, commit changes and return tuple (True, CMD_EXECUTION_SUCCESS).
        - If any exception occurs; rollback, do nothing on the database and return tuple (False, CMD_EXECUTION_FAILED).
    """

    def quit(self, seller):
        if seller is not None:
            res, msg = self.sign_out(seller)
            if res:
                return True, CMD_EXECUTION_SUCCESS
            else:
                return False, CMD_EXECUTION_FAILED
        return True, CMD_EXECUTION_SUCCESS

    """
        Retrieves all available plans and prints them.
        - Return type is a tuple, 1st element is a boolean and 2nd element is the response message from messages.py.
        - If the operation is successful; print available plans and return tuple (True, CMD_EXECUTION_SUCCESS).
        - If any exception occurs; return tuple (False, CMD_EXECUTION_FAILED).
        
        Output should be like:
        #|Name|Max Sessions|Max Stocks Per Product
        1|Basic|2|4
        2|Advanced|4|8
        3|Premium|6|12
    """

    def show_plans(self):
        try:
            cursor = self.conn.cursor()
            cursor.execute("SELECT plan_id, plan_name, max_parallel_sessions, max_stock_per_product FROM "
                           "subscription_plans;")
            queryPlans = cursor.fetchall()
            if queryPlans is None:
                # Fail if no plan exists (somehow)
                return False, CMD_EXECUTION_FAILED
            print("#|Name|Max Sessions|Max Stocks Per Product")
            for row in queryPlans:
                print("{0}|{1}|{2}|{3}".format(row[0], row[1], row[2], row[3]))
            cursor.close()
            return True, CMD_EXECUTION_SUCCESS
        except Exception as e:
            return False, CMD_EXECUTION_FAILED

    def show_subscription(self, seller):
        try:
            cursor = self.conn.cursor()
            cursor.execute("SELECT plan_id "
                           "FROM seller_subscription "
                           "WHERE seller_id = %s",
                           (seller.seller_id,))
            queryCustomerPlanId = cursor.fetchall()
            if queryCustomerPlanId is None:
                # Fail if no such customer exists (somehow)
                return False, CMD_EXECUTION_FAILED
            else:
                cursor.execute("SELECT plan_id, plan_name, max_parallel_sessions, max_stock_per_product "
                               "FROM subscription_plans "
                               "WHERE plan_id = %s",
                               (queryCustomerPlanId[0],))
                queryPlan = cursor.fetchone()
                if queryPlan is None:
                    # Fail if no such plan exists
                    return False, CMD_EXECUTION_FAILED
                print("#|Name|Max Sessions|Max Stocks Per Product")
                print("{0}|{1}|{2}|{3}".format(queryPlan[0], queryPlan[1], queryPlan[2], queryPlan[3]))
                cursor.close()
                return True, CMD_EXECUTION_SUCCESS
        except Exception as e:
            return False, CMD_EXECUTION_FAILED

    """
        Change stock count of a product.
        - Return type is a tuple, 1st element is a seller object and 2nd element is the response message from messages.py.
        - If target product does not exist on the database, return tuple (False, PRODUCT_NOT_FOUND).
        - If target stock count > current plan's max_stock_per_product, return tuple (False, QUOTA_LIMIT_REACHED).
        - If the operation is successful, commit changes and return tuple (seller, CMD_EXECUTION_SUCCESS).
        - If any exception occurs; rollback, do nothing on the database and return tuple (False, CMD_EXECUTION_FAILED).
    """

    def change_stock(self, seller, product_id, change_amount):
        try:
            cursor = self.conn.cursor()
            cursor.execute("SELECT stock_count "
                           "FROM seller_stocks "
                           "WHERE seller_id = %s and product_id = %s",
                           (seller.seller_id, product_id,))
            querychange_stock = cursor.fetchone()
            if querychange_stock is None:
                # Fail if such a customer dne or his/her session count is 0 somehow
                return False, PRODUCT_NOT_FOUND
            cursor.execute("SELECT max_stock_per_product "
                           "FROM subscription_plans "
                           "WHERE plan_id = %s",
                           (seller.plan_id,))
            querymax_stock_per_product = cursor.fetchone()
            if querychange_stock[0] + change_amount < 0 or querychange_stock[0] + change_amount > \
                    querymax_stock_per_product[0]:
                return False, STOCK_UPDATE_FAILURE

            cursor.execute("""
                                       UPDATE seller_stocks
                                       SET stock_count = stock_count + (%s) WHERE seller_id = %s and product_id = %s;
                                       """,
                           (change_amount, seller.seller_id, product_id,))
            self.conn.commit()
            cursor.close()
            return True, CMD_EXECUTION_SUCCESS


        except Exception as e:
            return False, CMD_EXECUTION_FAILED

    """
        Retrieves authenticated seller's remaining quota for stocks and prints it. 
        - Return type is a tuple, 1st element is a boolean and 2nd element is the response message from messages.py.
        - If the operation is successful; print the authenticated seller's quota and return tuple (True, CMD_EXECUTION_SUCCESS).
        - If any exception occurs; return tuple (False, CMD_EXECUTION_FAILED).

        If the seller is subscribed to a plan with max_stock_per_product = 12 and
        the current stock for product 92bf5d2084dfbcb57d9db7838bac5cd0 is 10, then output should be like:
        
        Product Id|Remaining Quota
        92bf5d2084dfbcb57d9db7838bac5cd0|2

        If the seller does not have a stock, print 'Quota limit is not activated yet.'
    """

    def show_quota(self, seller):
        try:
            cursor = self.conn.cursor()
            cursor.execute("SELECT product_id , stock_count "
                           "FROM seller_stocks "
                           "WHERE seller_id = %s",
                           (seller.seller_id,))
            queryshow_quota = cursor.fetchall()
            if queryshow_quota is None:
                return False, QUOTA_INACTIVE
            cursor.execute("SELECT max_stock_per_product "
                           "FROM subscription_plans "
                           "WHERE plan_id = %s",
                           (seller.plan_id,))
            max_stock_per_product_show_quota = cursor.fetchone()
            print("Product Id|Remaining Quota")
            for records in queryshow_quota:
                print("{0}|{1}".format(records[0], max_stock_per_product_show_quota[0] - records[1]))
            return True, CMD_EXECUTION_SUCCESS

        except Exception as e:
            return False, CMD_EXECUTION_FAILED

    """
        Subscribe authenticated seller to new plan.
        - Return type is a tuple, 1st element is a seller object and 2nd element is the response message from messages.py.
        - If target plan does not exist on the database, return tuple (None, PRODUCT_NOT_FOUND).
        - If the new plan's max_parallel_sessions < current plan's max_parallel_sessions, return tuple (None, SUBSCRIBE_MAX_PARALLEL_SESSIONS_UNAVAILABLE).
        - If the operation is successful, commit changes and return tuple (seller, CMD_EXECUTION_SUCCESS).
        - If any exception occurs; rollback, do nothing on the database and return tuple (None, CMD_EXECUTION_FAILED).
    """

    def subscribe(self, seller, plan_id):
        try:
            cursor = self.conn.cursor()
            cursor.execute("SELECT max_parallel_sessions "
                           "FROM subscription_plans "
                           "WHERE plan_id = %s",
                           (seller.plan_id,))
            queryplanid_old = cursor.fetchone()
            cursor.execute("SELECT max_parallel_sessions "
                           "FROM subscription_plans "
                           "WHERE plan_id = %s",
                           (plan_id,))
            queryplanid_new = cursor.fetchone()
            if queryplanid_new is None:
                return None, SUBSCRIBE_PLAN_NOT_FOUND
            if queryplanid_old[0] > queryplanid_new[0]:
                return None, SUBSCRIBE_MAX_PARALLEL_SESSIONS_UNAVAILABLE
            cursor.execute("""
                                                   UPDATE seller_subscription
                                                   SET plan_id = %s WHERE  seller_id = %s;
                                                   """,
                           (plan_id, seller.seller_id,))
            self.conn.commit()
            cursor.close()
            seller.plan_id = plan_id
            return seller, CMD_EXECUTION_SUCCESS
        except Exception as e:
            return None, CMD_EXECUTION_FAILED

    """
        Change stock amounts for multiple distinct products.
        - Return type is a tuple, 1st element is a boolean and 2nd element is the response message from messages.py.
        - If everything is OK and the operation is successful, return (True, CMD_EXECUTION_SUCCESS).
        - If the operation is successful, commit changes and return tuple (True, CMD_EXECUTION_SUCCESS).
        - If any one of the product ids is incorrect; rollback, do nothing on the database and return tuple (False, CMD_EXECUTION_FAILED).
        - If any one of the products is not in the stock; rollback, do nothing on the database and return tuple (False, CMD_EXECUTION_FAILED).
        - If any exception occurs; rollback, do nothing on the database and return tuple (False, CMD_EXECUTION_FAILED).
    """

    def ship(self, seller, product_ids):
        try:
            cursor = self.conn.cursor()
            for order in product_ids:

                cursor.execute("SELECT * "
                               "FROM seller_stocks "
                               "WHERE product_id = %s and seller_id = %s and stock_count > 0",
                               (order, seller.seller_id))
                queryplanid_new = cursor.fetchone()
                if queryplanid_new is None:
                    return False, CMD_EXECUTION_FAILED

            for order in product_ids:
                exec_status, exec_message = self.change_stock(seller, order, -1 * int(1))

            if exec_status == True:
                return True, CMD_EXECUTION_SUCCESS
            else:
                return False, CMD_EXECUTION_FAILED
        except Exception as e:
            return False, CMD_EXECUTION_FAILED
    """
        Retrieves the gross income per product category for every month.
        - Return type is a tuple, 1st element is a boolean and 2nd element is the response message from messages.py.
        - If the operation is successful; print the results and return tuple (True, CMD_EXECUTION_SUCCESS).
        - If any exception occurs; return tuple (False, CMD_EXECUTION_FAILED).
        
        Output should be like:
        Gross Income|Year|Month
        123.45|2018|1
        67.8|2018|2
    """

    def calc_gross(self, seller):
        try:
            cursor = self.conn.cursor()
            cursor.execute(
                "select (s.price-s.freight_value) as gross ,s.year,s.month from (select SUM(oi.price) as price ,"
                "SUM(oi.freight_value) as freight_value ,EXTRACT(YEAR FROM o.order_purchase_timestamp) AS year,"
                "EXTRACT(MONTH FROM o.order_purchase_timestamp) AS month  from order_items oi, orders o where "
                "oi.order_id ="
                "o.order_id and oi.seller_id = %s"
                "group BY year, month) s",
                (seller.seller_id,))
            queryplanid_new = cursor.fetchall()
            if queryplanid_new is None:
                print("Gross Income: 0")
            print("Gross Income|Year|Month")
            for row in queryplanid_new:
                print("{0}|{1}|{2}".format(row[0], row[1], row[2]))
            cursor.close()
            return True, CMD_EXECUTION_SUCCESS
        except Exception as e:
            return False, CMD_EXECUTION_FAILED

    """
        Retrieves items on the customer's shopping cart
        - Return type is a tuple, 1st element is a boolean and 2nd element is the response message from messages.py.
        - If the operation is successful; print items on the cart and return tuple (True, CMD_EXECUTION_SUCCESS).
        - If any exception occurs; return tuple (False, CMD_EXECUTION_FAILED).
        
        Output should be like:
        Seller Id|Product Id|Amount
        dd7ddc04e1b6c2c614352b383efe2d36|e5f2d52b802189ee658865ca93d83a8f|2
        5b51032eddd242adc84c38acab88f23d|c777355d18b72b67abbeef9df44fd0fd|3
        df560393f3a51e74553ab94004ba5c87|ac6c3623068f30de03045865e4e10089|1
    """

    def show_cart(self, customer_id):
        try:
            cursor = self.conn.cursor()
            cursor.execute(
                "select * from customer_carts cc where cc.customer_id = %s",
                (customer_id,))
            queryplanid_new = cursor.fetchall()
            if queryplanid_new is None:
                return False, CUSTOMER_NOT_FOUND
            print("Seller Id|Product Id|Amount")
            for row in queryplanid_new:
                print("{0}|{1}|{2}".format(row[1], row[2], row[3]))
            return True, CMD_EXECUTION_SUCCESS

        except Exception as e:
            return False, CMD_EXECUTION_FAILED


    """
        Change count of items in shopping cart
        - Return type is a tuple, 1st element is a seller object and 2nd element is the response message from messages.py.
        - If customer does not exist on the database, return tuple (False, CUSTOMER_NOT_FOUND).
        - If target product does not exist on the database, return tuple (False, PRODUCT_NOT_FOUND).
        - If the operation is successful, commit changes and return tuple (True, CMD_EXECUTION_SUCCESS).
        - If any exception occurs; rollback, do nothing on the database and return tuple (False, CMD_EXECUTION_FAILED).
        - Consider stocks of sellers when you add items to the cart.
    """

    def change_cart(self, customer_id, product_id, seller_id, change_amount):
        cursor = self.conn.cursor()
        cursor.execute(
            "select * from customer_carts cc where cc.customer_id = %s",
            (customer_id,))
        queryplanid_new = cursor.fetchone()
        cursor.execute(
            "select * from customer_carts cc where cc.product_id = %s",
            (product_id,))
        queryplanid_new2 = cursor.fetchone()
        if queryplanid_new is None:
            return False, CUSTOMER_NOT_FOUND
        if queryplanid_new2 is None:
            return False, PRODUCT_NOT_FOUND
        cursor.execute(
            "select ss.stock_count from seller_stocks ss where ss.product_id = %s",
            (product_id,))
        queryplanid_new3 = cursor.fetchone()
        if queryplanid_new3 is None:
            return False, STOCK_UNAVAILABLE
        if (int(queryplanid_new3[0]) < change_amount) and change_amount > 0 :
            return False,STOCK_UNAVAILABLE

        cursor.execute(
            "select * from customer_carts cc where cc.product_id = %s and cc.customer_id = %s",
            (product_id,customer_id,))
        queryplanid_new4 = cursor.fetchone()
        if int(queryplanid_new4[3]) + int(change_amount) <= 0:
            cursor.execute(
                "DELETE FROM customer_carts cc WHERE cc.customer_id = %s and cc.product_id = %s;",
                (customer_id,product_id,))
            self.conn.commit()
            cursor.close()
            return True, CMD_EXECUTION_SUCCESS
        cursor.execute("""
                                                           UPDATE customer_carts
                                                           SET amount = %s WHERE  customer_id = %s and product_id = %s;
                                                           """,
                       (int(queryplanid_new4[3]) + int(change_amount),customer_id ,product_id, ))
        self.conn.commit()
        cursor.close()
        return True, CMD_EXECUTION_SUCCESS






    """
        Purchases items on the cart
        - Return type is a tuple, 1st element is a boolean and 2nd element is the response message from messages.py.
        - If the operation is successful; return tuple (True, CMD_EXECUTION_SUCCESS).
        - If any exception occurs; return tuple (False, CMD_EXECUTION_FAILED).
        
        Actions:
        - Change stocks on seller_stocks table
        - Remove entries from customer_carts table
        - Add entries to order_items table
        - Add single entry to order table
    """

    def purchase_cart(self, customer_id):
        cursor = self.conn.cursor()
        cursor.execute(
            "select  * from customer_carts cc where cc.customer_id  = %s",
            (customer_id,))
        queryplanid_new = cursor.fetchall()
        if queryplanid_new is None:
            return False, CUSTOMER_NOT_FOUND
        for order in queryplanid_new:

            exec_status, exec_message = self.change_stock(Seller(order[1], 0,0) , order, -1 * int(3))

            if exec_message == STOCK_UPDATE_FAILURE:
                return False, STOCK_UPDATE_FAILURE
            if exec_message == PRODUCT_NOT_FOUND:
                return False, PRODUCT_NOT_FOUND
            cursor.execute(
                "DELETE FROM customer_carts cc WHERE cc.customer_id = %s ;",
                (customer_id,))
            self.conn.commit()
            cursor.execute(
                "INSERT INTO order_items (order_id, order_item_id, product_id, seller_id, shipping_limit_date, price, "
                "freight_value) VALUES( %s,NULL , %s, %s,NULL ,NULL ,NULL );",
                ('xxxxxxxxxxxnewxxxxxxxxxxx',order[2],order[1],))
            self.conn.commit()
            cursor.execute(
                "INSERT INTO orders (order_id, customer_id, order_status, order_purchase_timestamp, "
                "order_approved_at, order_delivered_carrier_date, order_delivered_customer_date, "
                "order_estimated_delivery_date) VALUES(%s, %s, 'delivered', NULL, NULL, NULL, NULL, NULL);",
                ('xxxxxxxxxxxnewxxxxxxxxxxx', order[0],))
            self.conn.commit()
            cursor.close()
        return True,CMD_EXECUTION_SUCCESS

        return False, CMD_EXECUTION_FAILED
