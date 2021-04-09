

class order:

  # the bot's database id
  order_id = None
  # the exchange's id
  tradeId = None
  # the price to trade at
  price = 0.0
  # the market to trade in etc BTCUSDT
  market = ''
  # the amount being requested to trade ( denominated in the 1st currency in the market name )
  amount = 0.0
  # the value of amount converted to the 2nd currency in the market name
  deal_money = 0.0
  # the fee for this transaction if successful
  fee = 0.0
  # datetime object of when the order was created with the exchange
  create_time = None
  # is it a buy or sell
  action = ''
  # datetime object of when the order was fufilled or canceled
  finished_time = None
  # the amount actually traded
  deal_amount = 0.0
  # name for the type of trigger for this trade ( action, expirring, goal, cet, etc )
  bot_type = ''
  # status of the trade
  # 1. initalized
  # 2. requested
  # 3. not_deal / placed but not yet fufilled
  # 4. done
  # 5. canceled
  status = ''
  # datetime object of when the order was created in bot code
  request_time = None
  # number of times the price entered and left the green zone before trade completed
  greenTouches = 0
  # fee charged if order role is maker
  maker_fee = 0.0
  # fee chated if order role is taker
  taker_fee = 0.0
  # did the order provide liquidity or take liquidity
  role = ''
  # what currency was the fee paid in
  fee_asset = ''

  def __init__(self, market, botType, price, amount, action):
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
