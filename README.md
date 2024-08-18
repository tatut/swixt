# Swixt: a SWI-Prolog interface to XTDB v2

[XTDB](https://xtdb.com) is a temporal schema free database.

This library provides a SWI-Prolog interface to store and query Prolog
dicts as documents in XTDB.

**WIP: very much alpha as XTDB v2 is also still in early access**

## Usage

This library uses the XTDB v2 [HTTP API](https://docs.xtdb.com/drivers/http/openapi/index.html) to
post transactions and queries.

A database row is represented as a [key/value dict](https://www.swi-prolog.org/pldoc/man?section=bidicts)
Prolog where the tag is the name of the table.
Queries are done by providing a candidate dict.

```prolog
q(products{name: like("%leather")}, R).
R = [products{'_id':1, description:"A fine leather jacket for all pirating needs", name:"Fine leather jacket", price:29.95}]
```

Fields can be direct values (which are added as `=` where clauses) or operations.
Supported operations are: `=`, `<`, `<=`, `>`, `>=`, `like`, and `between`.

### Nested queries

XTDB has a handy way to query nested data much more conveniently than regular SQL joins.
If a value is another dict, it is interpreted as a nested subquery to return. By default the items
are returned in a nested list (using NEST_MANY), but you can specify `'_cardinality': one` to return
only 1 item (using NEST_ONE), in which case the subquery must only return one row.

You can refer to the parent via `^(parent_field)` to specify join condition.

This example fetches orders and pulls the customer information, each order line and even
the product information all in a single query:

```prolog
q(orders{customer: customer{'_id': ^(customer_id), '_cardinality': one},
         orderlines: orderline{order_id: ^('_id'),
                               product: products{'_cardinality': one,
                                                 '_id': ^(product_id),
                                                 '_only':[name,price]}}},
  R).
R = [orders{'_id':1,
            customer: customer{'_id':1, address:data{country:"FI", postalcode:"90210", street:"Somestreet 6"}, name:"Max Feedpressure"},
            customer_id:1,
            datetime: datetime(2024,8,14,16,40,55,666),
            orderlines:[orderline{'_id':2, order_id:1, product: products{name:"Ball", price: 7.99}, product_id:4, quantity:10},
                        orderline{'_id':1, order_id:1, product: products{name:"Fine leather jacket", 29.95}, product_id:1, quantity:1}]}]
```
