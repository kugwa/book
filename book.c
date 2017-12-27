/*
 * Data Structure to Store Order Book Information in Redis
 *
 * bid_prices (sorted_set):
 *     This data structure contains all the unmatched bid prices. The score of
 *     a member is the same as the member itself. In this way, Redis will sort
 *     the prices for us automatically.
 *
 * bid_amounts@[PRICE] (list):
 *     bid_amounts@[PRICE] exists if and only if PRICE is a member of bid_prices.
 *     It is a FIFO queue of bid orders at PRICE and its elements represent bid
 *     amounts.
 *
 * bid_users@[PRICE] (list):
 *     Similar to bid_amounts@[PRICE] except the elements of bid_users@[PRICE]
 *     represent bid users.
 *
 * ask_prices, ask_amounts@[PRICE], and ask_users@[PRICE]:
 *     The ask version of the above data structures.
 *
 * matched_[FIELD] (list):
 *     A FIFO queue of matched FIELD. FIELD can be bidders, bidprices, askers,
 *     askprices, amounts, and timestamps.
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>
#include <hiredis/hiredis.h>
#include <json-c/json.h>

static redisContext *context;

/*
 * Convert a binary-safe string into a null-terminated string.
 *
 * reply->str: a binary-safe string
 * return: a null-terminated string, which is available until the next call.
 */
static const char *get_reply_str(redisReply *reply)
{
    static int len = 0;
    static char *str = NULL;

    if (reply->type != REDIS_REPLY_STRING) return NULL;
    if (reply->len + 1 > len) {
        len = reply->len + 1;
        str = realloc(str, len);
    }
    memcpy(str, reply->str, reply->len);
    str[reply->len] = '\0';
    return str;
}

static inline double get_reply_double(redisReply *reply)
{
    return atof(get_reply_str(reply));
}

/*
 * Add an order.
 *
 * cmd: "bid" or "ask"
 */
static void bid_ask(const char *cmd, const char *user,
                    double price, double amount)
{
    redisReply *reply;
    reply = redisCommand(context, "ZADD %s_prices %f %f", cmd, price, price);
    freeReplyObject(reply);
    reply = redisCommand(context, "RPUSH %s_users@%f %s", cmd, price, user);
    freeReplyObject(reply);
    reply = redisCommand(context, "RPUSH %s_amounts@%f %f", cmd, price, amount);
    freeReplyObject(reply);
}

/*
 * Remove all data in Redis.
 */
static void clear()
{
    redisReply *prices, *reply;
    int which, i;
    const char *bid_ask_str[2] = {"bid", "ask"};

    for (which = 0; which < 2; which++) {
        prices = redisCommand(context, "ZRANGE %s_prices 0 -1",
                              bid_ask_str[which]);
        for (i = 0; i < prices->elements; i++) {
            const char *price = get_reply_str(prices->element[i]);
            reply = redisCommand(context, "DEL %s_users@%s %s_amounts@%s",
                                 bid_ask_str[which], price,
                                 bid_ask_str[which], price);
            freeReplyObject(reply);
        }
        freeReplyObject(prices);
        reply = redisCommand(context, "DEL %s_prices", bid_ask_str[which]);
        freeReplyObject(reply);
    }
    reply = redisCommand(context, "DEL "
                                  "matched_bidders "
                                  "matched_bidprices "
                                  "matched_askers "
                                  "matched_askprices "
                                  "matched_amounts "
                                  "matched_timestamps");
    freeReplyObject(reply);
}

/*
 * List all unmatched prices.
 */
static void list()
{
    redisReply *prices, *reply;
    json_object *list, *reqs, *row;
    char s[20];
    int i;
    double total;

    list = json_object_new_object();

    int which;
    const char *get_prices_cmd[2] = {"ZRANGE bid_prices 0 -1",
                                     "ZREVRANGE ask_prices 0 -1"},
               *get_amounts_cmd[2] = {"LRANGE bid_amounts@%f 0 -1",
                                      "LRANGE ask_amounts@%f 0 -1"},
               *json_col[2] = {"bids", "asks"};

    for (which = 0; which < 2; which++) {
        reqs = json_object_new_array();
        prices = redisCommand(context, get_prices_cmd[which]);
        for (total = 0, i = prices->elements - 1; i >= 0; i--) {
            double price = get_reply_double(prices->element[i]);
            reply = redisCommand(context, get_amounts_cmd[which], price);
            int j;
            double amount = 0;
            for (j = 0; j < reply->elements; j++) {
                amount += get_reply_double(reply->element[j]);
            }
            total += amount;

            row = json_object_new_object();
            snprintf(s, 20, "%zu", reply->elements);
            json_object_object_add(row, "count", json_object_new_string(s));
            snprintf(s, 20, "%.2lf", amount);
            json_object_object_add(row, "amount", json_object_new_string(s));
            snprintf(s, 20, "%.2lf", total);
            json_object_object_add(row, "total", json_object_new_string(s));
            snprintf(s, 20, "%.2lf", price);
            json_object_object_add(row, "price", json_object_new_string(s));
            json_object_array_add(reqs, row);

            freeReplyObject(reply);
        }
        json_object_object_add(list, json_col[which], reqs);
        freeReplyObject(prices);
    }

    puts(json_object_to_json_string_ext(list, JSON_C_TO_STRING_PRETTY));
    json_object_put(list);
}

/*
 * Trade the orders at given prices based on FIFO.
 * return: the number of trades
 *         *bid_fully_matched == 1 if bid_price is fully matched.
 *         *ask_fully_matched == 1 if ask_price is fully matched.
 */
static int trade(double bid_price, double ask_price,
                 int *bid_fully_matched, int *ask_fully_matched)
{
    redisReply *reply;
    double bid_amount, ask_amount, trade_amount;
    int trades = 0;

    while (1) {
        reply = redisCommand(context, "LINDEX bid_amounts@%f 0", bid_price);
        if (reply->type == REDIS_REPLY_NIL) {
            freeReplyObject(reply);
            reply = redisCommand(context, "ZREM bid_prices %f", bid_price);
            freeReplyObject(reply);
            *bid_fully_matched = 1;
        } else {
            bid_amount = get_reply_double(reply);
            freeReplyObject(reply);
            *bid_fully_matched = 0;
        }

        reply = redisCommand(context, "LINDEX ask_amounts@%f 0", ask_price);
        if (reply->type == REDIS_REPLY_NIL) {
            freeReplyObject(reply);
            reply = redisCommand(context, "ZREM ask_prices %f", ask_price);
            freeReplyObject(reply);
            *ask_fully_matched = 1;
        } else {
            ask_amount = get_reply_double(reply);
            freeReplyObject(reply);
            *ask_fully_matched = 0;
        }

        /* Stop when either bid price or ask price run out of amount. */
        if (*bid_fully_matched == 1 || *ask_fully_matched == 1) break;

        if (bid_amount > ask_amount) {
            trade_amount = ask_amount;
            ask_amount = 0;
            bid_amount -= trade_amount;
        } else if (ask_amount > bid_amount) {
            trade_amount = bid_amount;
            bid_amount = 0;
            ask_amount -= trade_amount;
        } else {
            bid_amount = 0;
            ask_amount = 0;
        }

        const char *user;
        reply = redisCommand(context, "LINDEX bid_users@%f 0", bid_price);
        user = get_reply_str(reply);
        freeReplyObject(reply);
        reply = redisCommand(context, "LPUSH matched_bidders %s", user);
        freeReplyObject(reply);
        reply = redisCommand(context, "LINDEX ask_users@%f 0", ask_price);
        user = get_reply_str(reply);
        freeReplyObject(reply);
        reply = redisCommand(context, "LPUSH matched_askers %s", user);
        freeReplyObject(reply);
        reply = redisCommand(context, "LPUSH matched_bidprices %f", bid_price);
        freeReplyObject(reply);
        reply = redisCommand(context, "LPUSH matched_askprices %f", ask_price);
        freeReplyObject(reply);
        reply = redisCommand(context, "LPUSH matched_amounts %f", trade_amount);
        freeReplyObject(reply);
        reply = redisCommand(context, "LPUSH matched_timestamps %ld", time(NULL));
        freeReplyObject(reply);

        trades++;

        if (bid_amount == 0) {
            reply = redisCommand(context, "LPOP bid_amounts@%f", bid_price);
            freeReplyObject(reply);
            reply = redisCommand(context, "LPOP bid_users@%f", bid_price);
            freeReplyObject(reply);
        } else {
            reply = redisCommand(context, "LSET bid_amounts@%f 0 %f",
                                 bid_price, bid_amount);
            freeReplyObject(reply);
        }

        if (ask_amount == 0) {
            reply = redisCommand(context, "LPOP ask_amounts@%f", ask_price);
            freeReplyObject(reply);
            reply = redisCommand(context, "LPOP ask_users@%f", ask_price);
            freeReplyObject(reply);
        } else {
            reply = redisCommand(context, "LSET ask_amounts@%f 0 %f",
                                 ask_price, ask_amount);
            freeReplyObject(reply);
        }
    }

    return trades;
}

/*
 * Eliminate the overlap between bid prices and ask prices.
 * return: the number of trades
 */
static int match()
{
    /* bid prices and ask prices */
    redisReply *bid_prices = redisCommand(context, "ZRANGE bid_prices 0 -1");
    if (bid_prices->elements == 0) {
        freeReplyObject(bid_prices);
        return 0;
    }
    redisReply *ask_prices = redisCommand(context, "ZRANGE ask_prices 0 -1");
    if (ask_prices->elements == 0) {
        freeReplyObject(ask_prices);
        return 0;
    }

    double price;

    /* indices of the lowest bid price which is above or equal to the lowest
       ask price, and the highest ask price which is below or equal to the
       highest bid price, respectively */
    int b_lb, a_ub;
    price = get_reply_double(ask_prices->element[0]);
    for (b_lb = bid_prices->elements - 1; b_lb >= 0 &&
         get_reply_double(bid_prices->element[b_lb]) >= price; b_lb--);
    b_lb++;
    price = get_reply_double(bid_prices->element[bid_prices->elements - 1]);
    for (a_ub = 0; a_ub < ask_prices->elements &&
         get_reply_double(ask_prices->element[a_ub]) <= price; a_ub++);
    a_ub--;

    /* no overlap between bid prices and ask prices */
    if (b_lb >= bid_prices->elements || a_ub < 0) {
        freeReplyObject(bid_prices);
        freeReplyObject(ask_prices);
        return 0;
    }

    /* indices to iterate bid prices and ask prices */
    int b = b_lb, a = 0;

    int trades = 0;

    while (1) {
        int bid_fully_matched, ask_fully_matched;
        trades += trade(get_reply_double(bid_prices->element[b]),
                        get_reply_double(ask_prices->element[a]),
                        &bid_fully_matched, &ask_fully_matched);
        if (ask_fully_matched) {
            a++;
            if (a > a_ub) break;
            if (get_reply_double(bid_prices->element[b]) <
                get_reply_double(ask_prices->element[a])) {
                do b++; while(get_reply_double(bid_prices->element[b]) <
                              get_reply_double(ask_prices->element[a]));
            }
        }
        if (bid_fully_matched) {
            b++;
            if (b >= bid_prices->elements) break;
        }
    }

    freeReplyObject(bid_prices);
    freeReplyObject(ask_prices);

    return trades;
}

void history(int start, int stop)
{
    redisReply *bidders, *bidprices, *askers, *askprices, *amounts, *timestamps;
    json_object *list, *matched;
    int i, num;
    char s[20];

    list = json_object_new_array();

    bidders = redisCommand(context, "LRANGE matched_bidders %d %d",
                           start, stop);
    bidprices = redisCommand(context, "LRANGE matched_bidprices %d %d",
                             start, stop);
    askers = redisCommand(context, "LRANGE matched_askers %d %d",
                          start, stop);
    askprices = redisCommand(context, "LRANGE matched_askprices %d %d",
                             start, stop);
    amounts = redisCommand(context, "LRANGE matched_amounts %d %d",
                           start, stop);
    timestamps = redisCommand(context, "LRANGE matched_timestamps %d %d",
                              start, stop);

    for (num = bidders->elements, i = 0; i < num; i++) {
        matched = json_object_new_object();
        
        json_object_object_add(matched, "bidder",
            json_object_new_string(get_reply_str(bidders->element[i])));
        snprintf(s, 20, "%.2lf", get_reply_double(bidprices->element[i]));
        json_object_object_add(matched, "bidprice", json_object_new_string(s));
        json_object_object_add(matched, "asker",
            json_object_new_string(get_reply_str(askers->element[i])));
        snprintf(s, 20, "%.2lf", get_reply_double(askprices->element[i]));
        json_object_object_add(matched, "askprice", json_object_new_string(s));
        snprintf(s, 20, "%.2lf", get_reply_double(amounts->element[i]));
        json_object_object_add(matched, "amount", json_object_new_string(s));
        json_object_object_add(matched, "timestamp",
            json_object_new_string(get_reply_str(timestamps->element[i])));
        
        json_object_array_add(list, matched);
    }

    freeReplyObject(bidders);
    freeReplyObject(bidprices);
    freeReplyObject(askers);
    freeReplyObject(askprices);
    freeReplyObject(amounts);
    freeReplyObject(timestamps);

    puts(json_object_to_json_string_ext(list, JSON_C_TO_STRING_PRETTY));
    json_object_put(list);
}

/*
 * argv[0]: command
 * argv[1] ~ argv[argc - 1]: arguments
 */
static void process_command(int argc, char **argv)
{
    if (argc == 0) return;
    if (strcmp(argv[0], "bid") == 0 || strcmp(argv[0], "ask") == 0) {
        if (argc != 4) {
            printf("usage: %s [USER] [PRICE] [AMOUNT]\n", argv[0]);
            return;
        }
        bid_ask(argv[0], argv[1], atof(argv[2]), atof(argv[3]));
    } else if (strcmp(argv[0], "clear") == 0) {
        clear();
    } else if (strcmp(argv[0], "list") == 0) {
        list();
    } else if (strcmp(argv[0], "match") == 0) {
        printf("%d\n", match());
    } else if (strcmp(argv[0], "history") == 0) {
        if (argc != 3) {
            puts("usage: history [START] [STOP]");
            return;
        }
        history(atoi(argv[1]), atoi(argv[2]));
    } else if (strcmp(argv[0], "help") == 0) {
        puts("bid [USER] [PRICE] [AMOUNT]   Bid AMOUNT at PRICE");
        puts("ask [USER] [PRICE] [AMOUNT]   Ask AMOUNT at PRICE");
        puts("list                          List all unmatched prices");
        puts("match                         Match bids and asks");
        puts("history [START] [STOP]        List STARTth to STOPth latest trades");
        puts("clear                         Remove all data in Redis");
        puts("help                          Show this help");
    } else {
        puts("unknown command");
    }
}

/*
 * argc == 1: Get commands from stdin.
 * argc >= 2: Get a command from command line arguments.
 */
int main(int argc, char **argv)
{
    context = redisConnect("127.0.0.1", 6379);
    if (context == NULL) {
        fprintf(stderr, "redisConnect failed\n");
        return 1;
    }
    if (context->err) {
        fprintf(stderr, "redisConnect: %s\n", context->errstr);
        return 1;
    }

    if (argc > 1) {
        process_command(argc - 1, argv + 1);
    } else {
        int i, book_argc;
        char *book_argv[5], _book_argv[5][10];
        for (i = 0; i < 5; i++) book_argv[i] = _book_argv[i];

        while (1) {
            /* prompt */
            if (isatty(fileno(stdin))) printf("book> ");
            char book_cmd[50];
            if (fgets(book_cmd, 50, stdin) == NULL) break;
            for (i = 0; book_cmd[i]; i++) {
                if (book_cmd[i] == '\n') book_cmd[i] = '\0';
            }

            /* parse commands */
            char *arg, *saveptr;
            arg = strtok_r(book_cmd, " ", &saveptr);
            for (book_argc = 0; book_argc < 5 && arg != NULL; book_argc++) {
                strncpy(book_argv[book_argc], arg, 10);
                arg = strtok_r(NULL, " ", &saveptr);
            }

            process_command(book_argc, book_argv);
        }
    }

    redisFree(context);
    return 0;
}
