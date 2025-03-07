/*
Implement a real-time Stock trading engine for matching Stock Buys with Stock Sells.
1. Write an ‘addOrder’ function that will have the following parameters:
    ‘Order Type’ (Buy or Sell), ‘Ticker Symbol’, ‘Quantity’, ‘Price’
    Support 1,024 tickers (stocks) being traded.
    Write a wrapper to have this ‘addOrder’ function randomly execute with different parameter values to
     simulate active stock transactions.

2. Write a ‘matchOrder’ function, that will match Buy & Sell orders with the following criteria:
    Buy price for a particular ticker is greater than or equal to lowest Sell price available then.
    Write your code to handle race conditions when multiple threads modify the Stock order book, as run in real-life,
     by multiple stockbrokers. Also, use lock-free data structures.
    Do not use any dictionaries, maps or equivalent data structures. Essentially there should be no ‘import’-s nor
     ‘include’-s nor similar construct relevant to the programming language you are using that provides you dictionary,
      map or equivalent data structure capability. In essence, you are writing the entire code. Standard language-
      specific non data structure related items are ok, but try to avoid as best as you can.
    Write your ‘matchOrder’ function with a time-complexity of O(n), where 'n' is the number of orders in the Stock
     order book.
 */

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;

// Order types: BUY and SELL.
enum Type {
    BUY,
    SELL
}


class Order {
    Type type;
    double price;
    int quantity;
    String ticker;
    AtomicReference<Order> next; // lock-free pointer to the next order

    public Order(Type type, double price, int quantity, String ticker) {
        this.type = type;
        this.price = price;
        this.quantity = quantity;
        this.ticker = ticker;
        this.next = new AtomicReference<>(null);
    }

    @Override
    public String toString() {
        return type + " Order: " + ticker + " | Quantity: " + quantity + " | Price: " + price;
    }
}

// StockTrade maintains separate order books (as lock-free sorted linked lists) for BUY and SELL orders.
// The books are stored in fixed arrays of size 1024.
class StockTrade {
    // Arrays of atomic head pointers for each ticker.
    private final AtomicReference<Order>[] buyHeads;
    private final AtomicReference<Order>[] sellHeads;

    @SuppressWarnings("unchecked")
    public StockTrade() {
        buyHeads = (AtomicReference<Order>[]) new AtomicReference[1024];
        sellHeads = (AtomicReference<Order>[]) new AtomicReference[1024];
        for (int i = 0; i < 1024; i++) {
            buyHeads[i] = new AtomicReference<>(null);
            sellHeads[i] = new AtomicReference<>(null);
        }
    }

    // extract index from ticker string "TICK" + number.
    private int getTickerIndex(String ticker) {
        try {
            return Integer.parseInt(ticker.substring(4));
        } catch (Exception e) {
            return 0; // default index if parsing fails
        }
    }


    public void addOrder(Type type, String ticker, int quantity, double price) {
        Order newOrder = new Order(type, price, quantity, ticker);
        System.out.println("Adding: " + newOrder);
        int index = getTickerIndex(ticker);
        if (type == Type.BUY) {
            addBuyOrder(index, newOrder);
        } else {
            addSellOrder(index, newOrder);
        }
    }

    // Insert a BUY order in descending order by price for a given ticker.
    private void addBuyOrder(int index, Order newOrder) {
        AtomicReference<Order> headRef = buyHeads[index];
        while (true) {
            Order head = headRef.get();
            if (head == null || newOrder.price > head.price) {
                newOrder.next.set(head);
                if (headRef.compareAndSet(head, newOrder)) {
                    break;
                }
            } else {
                Order pred = head;
                Order curr = head.next.get();
                while (curr != null && curr.price >= newOrder.price) {
                    pred = curr;
                    curr = curr.next.get();
                }
                newOrder.next.set(curr);
                if (pred.next.compareAndSet(curr, newOrder)) {
                    break;
                }
            }
        }
    }

    // Insert a SELL order in ascending order by price for a given ticker.
    private void addSellOrder(int index, Order newOrder) {
        AtomicReference<Order> headRef = sellHeads[index];
        while (true) {
            Order head = headRef.get();
            if (head == null || newOrder.price < head.price) {
                newOrder.next.set(head);
                if (headRef.compareAndSet(head, newOrder)) {
                    break;
                }
            } else {
                Order pred = head;
                Order curr = head.next.get();
                while (curr != null && curr.price <= newOrder.price) {
                    pred = curr;
                    curr = curr.next.get();
                }
                newOrder.next.set(curr);
                if (pred.next.compareAndSet(curr, newOrder)) {
                    break;
                }
            }
        }
    }

    // Match orders for a specific ticker.
    private void matchOrdersForTicker(int index) {
        AtomicReference<Order> buyRef = buyHeads[index];
        AtomicReference<Order> sellRef = sellHeads[index];

        while (true) {
            Order buy = buyRef.get();
            Order sell = sellRef.get();
            if (buy == null || sell == null) break;

            if (buy.price >= sell.price) {
                int matchedQuantity = Math.min(buy.quantity, sell.quantity);
                System.out.println(" Matched: " + matchedQuantity + " shares of " + buy.ticker + " at $" + sell.price);
                buy.quantity -= matchedQuantity;
                sell.quantity -= matchedQuantity;

                if (buy.quantity == 0) {
                    buyRef.compareAndSet(buy, buy.next.get());
                    System.out.println("Removing completed BUY order: " + buy);
                }
                if (sell.quantity == 0) {
                    sellRef.compareAndSet(sell, sell.next.get());
                    System.out.println("Removing completed SELL order: " + sell);
                }
            } else {
                break;
            }
        }
    }

    // Match orders for all 1,024 tickers.
    public void matchOrders() {
        System.out.println("Matching orders...");
        for (int i = 0; i < 1024; i++) {
            matchOrdersForTicker(i);
        }
    }
}

// Main simulates active transactions by randomly submitting orders from multiple threads.
public class Main {
    public static void main(String[] args) {
        StockTrade stockTrade = new StockTrade();
        ExecutorService orderExec = Executors.newFixedThreadPool(4);

        // Create a executor for matching orders concurrently.
        ExecutorService matchExec = Executors.newSingleThreadExecutor();
        matchExec.submit(() -> {

            while (!Thread.currentThread().isInterrupted()) {
                stockTrade.matchOrders();
                try {

                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        });

        // This task will submit 100 random orders.
        Runnable task = () -> {
            for (int i = 0; i < 100; i++) {
                Type type = (Math.random() < 0.5) ? Type.BUY : Type.SELL;
                String ticker = "TICK" + (int)(Math.random() * 1024);
                int quantity = (int)(Math.random() * 100) + 1;
                double price = Math.random() * 500;
                stockTrade.addOrder(type, ticker, quantity, price);
            }
        };

        try {
            orderExec.invokeAll(
                    java.util.Arrays.asList(
                            Executors.callable(task),
                            Executors.callable(task),
                            Executors.callable(task),
                            Executors.callable(task)
                    )
            );
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        orderExec.shutdown();
        try {
            orderExec.awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        System.out.println("All orders have been added.");

        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            System.out.println("Interrupted while waiting for orders to be added.");
            e.printStackTrace();
        }

        matchExec.shutdownNow();
        try {
            matchExec.awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}