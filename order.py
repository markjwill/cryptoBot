

class order:

    # the bot's database id
    orderId = None
    # the exchange's id
    tradeId = None
    # the price to trade at
    price = 0.0
    # the market to trade in etc BTCUSDT
    market = ''
    # the amount being requested to trade ( denominated in the 1st currency in the market name )
    amount = 0.0
    # the value of amount converted to the 2nd currency in the market name
    dealMoney = 0.0
    # the fee for this transaction if successful
    fee = 0.0
    # datetime object of when the order was created with the exchange
    createTime = None
    # is it a buy or sell
    action = ''
    # datetime object of when the order was fufilled or canceled
    finishedTime = None
    # the amount actually traded
    dealAmount = 0.0
    # name for the type of trigger for this trade ( action, expiring, goal, cet, etc )
    botType = ''
    # status of the trade
    # 1. initalized
    # 2. requested
    # 3. not_deal / placed but not yet fufilled
    # 4. done
    # 5. canceled
    status = ''
    # datetime object of when the order was created in bot code
    requestTime = None
    # number of times the price entered and left the green zone before trade completed
    greenTouches = 0
    # fee charged if order role is maker
    makerFee = 0.0
    # fee chated if order role is taker
    takerFee = 0.0
    # did the order provide liquidity or take liquidity
    role = ''
    # what currency was the fee paid in
    feeAsset = ''

    def new(self, market, botType, price, amount, action):
        if self.orderId is not None:
            print("Cannot call new() on an existing order", flush=True)
            return False
        self.botType = botType
        self.status = 'initalized'
        self.market = market
        self.amount = amount
        self.price = price
        self.action = action
        cur.execute(
                "INSERT INTO orders(bot_type, status, market, amount, price, type) VALUES ('{0:s}', '{1:s}', '{2:s}', {3:f}, {4:f}, '{5:s}')".format(self.botType, self.status, self.market, self.amount, self.price, self.action))
        conn.commit()
        cur.execute("SELECT LAST_INSERT_ID() FROM orders")
        values = cur.fetchall()
        self.order_id = values[0][0]
        cur.execute("SELECT request_time FROM orders WHERE order_id = {0:d}".format(self.order_id))
        values = cur.fetchall()
        self.requestTime = values[0][0]

    def load(self, orderId):
        if self.orderId is not None:
            print("Cannot call load() on an existing order", flush=True)
            return False
        self.order_id = order_id
        cur.execute("SELECT bot_type, status, market, amount, price, type, trade_id, deal_money, fee, create_time, finished_time, deal_amount, green_touches, maker_fee, taker_fee, role, fee_asset, request_time FROM orders WHERE order_id = {0:d}".format(self.order_id))
        value = cur.fetchall()
        self.bulkSet(value[0])

    def bulkSet(value)
        self.botType = value[0]
        self.status = value[1]
        self.market = value[2]
        self.amount = value[3]
        self.price = value[4]
        self.action = value[5]
        self.tradeId = value[6]
        self.dealMoney = value[7]
        self.fee = value[8]
        self.createTime = value[9]
        self.finishedTime = value[10]
        self.dealAmount = value[11]
        self.greenTouches = value[12]
        self.makerFee = value[13]
        self.takerFee = value[14]
        self.role = value[15]
        self.feeAsset = value[16]
        self.requestTime = value[17]

    def save(self):
        cur.execute(
                "UPDATE orders SET bot_type = '{0:s}', status = '{1:s}', market = '{2:s}', amount = {3:f}, price = {4:f}, type = '{5:s}', trade_id = {6:d}, deal_money = {7:f}, fee = {8:f}, create_time = '{9:s}', action = '{10:s}', finished_time = '{11:s}', deal_amount = {12:f}, green_touches = {13:d}, maker_fee = {14:f}, taker_fee = {15:f}, role = '{16:s}', fee_asset = '{17:s}' WHERE order_id = {18:d}".format(self.botType, self.status, self.market, self.amount, self.price, self.action, self.tradeId, self.dealMoney, self.fee, self.createTime, self.action, self.finishedTime, self.dealAmount, self.greenTouches, self.makerFee, self.takerFee, self.role, self.feeAsset, self.orderId))
        conn.commit()
