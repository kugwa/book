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
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <hiredis/hiredis.h>
#include <json-c/json.h>

static redisContext *context;

/*
 * Convert a binary-safe string into a null-terminated string.
 *
 * reply->str: a binary-safe string
 * return: a null-terminated string
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
    reply = redisCommand(context, "rpush %s_users@%f %s", cmd, price, user);
    freeReplyObject(reply);
    reply = redisCommand(context, "rpush %s_amounts@%f %f", cmd, price, amount);
    freeReplyObject(reply);
}

/*
 * Remove all data in Redis.
 */
static void clear()
{
    redisReply *prices, *reply;
    char *bid_ask[2] = {"bid", "ask"};
    int which, i;

    for (which = 0; which < 2; which++) {
        prices = redisCommand(context, "ZRANGE %s_prices 0 -1", bid_ask[which]);
        for (i = 0; i < prices->elements; i++) {
            reply = redisCommand(context, "DEL %s_users@%s",
                                 bid_ask[which],
                                 get_reply_str(prices->element[i]));
            freeReplyObject(reply);
            reply = redisCommand(context, "DEL %s_amounts@%s",
                                 bid_ask[which],
                                 get_reply_str(prices->element[i]));
            freeReplyObject(reply);
        }
        freeReplyObject(prices);
        reply = redisCommand(context, "DEL %s_prices", bid_ask[which]);
        freeReplyObject(reply);
    }
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

    reqs = json_object_new_array();
    prices = redisCommand(context, "ZRANGE bid_prices 0 -1");
    for (total = 0, i = prices->elements - 1; i >= 0; i--) {
        double price = get_reply_double(prices->element[i]);
        reply = redisCommand(context, "LRANGE bid_amounts@%f 0 -1", price);
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
    json_object_object_add(list, "bids", reqs);
    freeReplyObject(prices);

    reqs = json_object_new_array();
    prices = redisCommand(context, "ZREVRANGE ask_prices 0 -1");
    for (total = 0, i = prices->elements - 1; i >= 0; i--) {
        double price = get_reply_double(prices->element[i]);
        reply = redisCommand(context, "LRANGE ask_amounts@%f 0 -1", price);
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
    json_object_object_add(list, "asks", reqs);
    freeReplyObject(prices);

    printf("%s", json_object_to_json_string_ext(list, JSON_C_TO_STRING_PRETTY));
    json_object_put(list);
}

/*
 * Trade the orders at given prices based on FIFO.
 * return: *bid_fully_matched == 1 if bid_price is fully matched.
 *         *ask_fully_matched == 1 if ask_price is fully matched.
 */
static void trade(double bid_price, double ask_price,
                  int *bid_fully_matched, int *ask_fully_matched)
{
    redisReply *reply;
    double bid_amount, ask_amount;

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
            bid_amount -= ask_amount;
            ask_amount = 0;
        } else if (ask_amount > bid_amount) {
            ask_amount -= bid_amount;
            bid_amount = 0;
        } else {
            bid_amount = 0;
            ask_amount = 0;
        }

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
}

/*
 * Eliminate the overlap between bid prices and ask prices
 */
static void match()
{
    /* bid prices and ask prices */
    redisReply *bid_prices = redisCommand(context, "ZRANGE bid_prices 0 -1");
    if (bid_prices->elements == 0) {
        freeReplyObject(bid_prices);
        return;
    }
    redisReply *ask_prices = redisCommand(context, "ZRANGE ask_prices 0 -1");
    if (ask_prices->elements == 0) {
        freeReplyObject(ask_prices);
        return;
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
        return;
    }

    /* indices to iterate bid prices and ask prices */
    int b = b_lb, a = 0;

    while (1) {
        int bid_fully_matched, ask_fully_matched;
        trade(get_reply_double(bid_prices->element[b]),
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
        match();
    } else if (strcmp(argv[0], "help") == 0) {
        puts("bid [USER] [PRICE] [AMOUNT]   Bid AMOUNT at PRICE");
        puts("ask [USER] [PRICE] [AMOUNT]   Ask AMOUNT at PRICE");
        puts("clear                         Remove all data in Redis");
        puts("list                          List all unmatched prices");
        puts("match                         Match bids and asks");
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
