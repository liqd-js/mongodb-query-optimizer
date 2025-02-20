import { calculateDependencies, extractFields, optimizePipeline } from "../src";
import * as assert from "node:assert";

describe( 'extractFields', () =>
{
    it( 'should extract fields from $match', () =>
    {
        const stage = { "$match": { "status": "active" } };
        const { used, produced, destructive, removed } = extractFields( stage );

        assert.deepStrictEqual( used, ['status'] );
        assert.deepStrictEqual( produced, [] );
        assert.deepStrictEqual( removed, [] );
        assert.strictEqual( destructive, false );
    } );

    it( 'should extract fields from $match with math operator', () =>
    {
        const stage = { "$match": { "price": { "$gte": 100 } } };
        const { used, produced, destructive, removed } = extractFields( stage );

        assert.deepStrictEqual( used, ['price'] );
        assert.deepStrictEqual( produced, [] );
        assert.deepStrictEqual( removed, [] );
        assert.strictEqual( destructive, false );
    } );

    it( 'should extract fields from $match with logic operator', () =>
    {
        const stage = { "$match": { "$or": [{ "category": "books" }, { "category": "electronics" }] } };
        const { used, produced, destructive, removed } = extractFields( stage );

        assert.deepStrictEqual( used, ['category'] );
        assert.deepStrictEqual( produced, [] );
        assert.deepStrictEqual( removed, [] );
        assert.strictEqual( destructive, false );
    } );

    it( 'should extract fields from $match with array operator', () =>
    {
        const stage = { "$match": { "tags": { "$in": ["sale", "popular"] } } };
        const { used, produced, destructive, removed } = extractFields( stage );

        assert.deepStrictEqual( used, ['tags'] );
        assert.deepStrictEqual( produced, [] );
        assert.deepStrictEqual( removed, [] );
        assert.strictEqual( destructive, false );
    } );

    it( 'should extract fields from $match with exists operator', () =>
    {
        const stage = { "$match": { "nested.field": { "$exists": true } } };
        const { used, produced, destructive, removed } = extractFields( stage );

        assert.deepStrictEqual( used, ['nested.field'] );
        assert.deepStrictEqual( produced, [] );
        assert.deepStrictEqual( removed, [] );
        assert.strictEqual( destructive, false );
    } );

    it( 'should extract fields from $project', () =>
    {
        const stage = { "$project": { "name": 1, "price": 1 } };
        const { used, produced, destructive, removed } = extractFields( stage );

        assert.deepStrictEqual( used, ['name', 'price'] );
        assert.deepStrictEqual( produced, ['name', 'price'] );
        assert.deepStrictEqual( removed, [] );
        assert.strictEqual( destructive, false );
    } );

    it( 'should extract fields from $project with $concat', () =>
    {
        const stage = { "$project": { "fullName": { "$concat": ["$firstName", " ", "$lastName"] } } };
        const { used, produced, destructive, removed } = extractFields( stage );

        assert.deepStrictEqual( used, ['firstName', 'lastName'] );
        assert.deepStrictEqual( produced, ['fullName'] );
        assert.deepStrictEqual( removed, [] );
        assert.strictEqual( destructive, false );
    } );

    it( 'should extract fields from $project with math operator', () =>
    {
        const stage = { "$project": { "discountedPrice": { "$multiply": ["$price", 0.9] }, "_id": 0 } };
        const { used, produced, destructive, removed } = extractFields( stage );

        assert.deepStrictEqual( used, ['price'] );
        assert.deepStrictEqual( produced, ['discountedPrice'] );
        assert.deepStrictEqual( removed, ['_id'] );
        assert.strictEqual( destructive, false );
    } );

    it( 'should extract fields from $project with literal', () =>
    {
        const stage = { "$project": { "tags": 1, "extra": { "$literal": "static_value" } } };
        const { used, produced, destructive, removed } = extractFields( stage );

        assert.deepStrictEqual( used, ['tags'] );
        assert.deepStrictEqual( produced, ['tags', 'extra'] );
        assert.deepStrictEqual( removed, [] );
        assert.strictEqual( destructive, false );
    } );

    it( 'should extract fields from $project with condition', () =>
    {
        const stage = { "$project": { "name": 1, "price": { "$cond": [{ "$gt": ["$price", 100] }, "expensive", "cheap"] } } };
        const { used, produced, destructive, removed } = extractFields( stage );

        assert.deepStrictEqual( used, ['name', 'price'] );
        assert.deepStrictEqual( produced, ['name', 'price'] );
        assert.deepStrictEqual( removed, [] );
        assert.strictEqual( destructive, false );
    } );

    it( 'should extract fields from $group', () =>
    {
        const stage = { "$group": { "_id": "$category", "total": { "$sum": "$amount" } } };
        const { used, produced, destructive, removed } = extractFields( stage );

        assert.deepStrictEqual( used, ['category', 'amount'] );
        assert.deepStrictEqual( produced, ['_id', 'total'] );
        assert.deepStrictEqual( removed, [] );
        assert.strictEqual( destructive, false );
    } );

    it( 'should extract fields from $group with math operator', () =>
    {
        const stage = { "$group": { "_id": null, "avgPrice": { "$avg": "$price" } } };
        const { used, produced, destructive, removed } = extractFields( stage );

        assert.deepStrictEqual( used, ['price'] );
        assert.deepStrictEqual( produced, ['_id', 'avgPrice'] );
        assert.deepStrictEqual( removed, [] );
        assert.strictEqual( destructive, false );
    } );

    it( 'should extract fields from $group with date operator', () =>
    {
        const stage = { "$group": { "_id": { "year": { "$year": "$date" } }, "count": { "$sum": 1 } } };
        const { used, produced, destructive, removed } = extractFields( stage );

        assert.deepStrictEqual( used, ['date'] );
        assert.deepStrictEqual( produced, ['_id', 'count'] );
        assert.deepStrictEqual( removed, [] );
        assert.strictEqual( destructive, false );
    } );

    it( 'should extract fields from $group with array operator', () =>
    {
        const stage = { "$group": { "_id": "$userId", "items": { "$push": "$item" } } };
        const { used, produced, destructive, removed } = extractFields( stage );

        assert.deepStrictEqual( used, ['userId', 'item'] );
        assert.deepStrictEqual( produced, ['_id', 'items'] );
        assert.deepStrictEqual( removed, [] );
        assert.strictEqual( destructive, false );
    } );

    it( 'should extract fields from $group with max operator', () =>
    {
        const stage = { "$group": { "_id": "$department", "maxSalary": { "$max": "$salary" } } };
        const { used, produced, destructive, removed } = extractFields( stage );

        assert.deepStrictEqual( used, ['department', 'salary'] );
        assert.deepStrictEqual( produced, ['_id', 'maxSalary'] );
        assert.deepStrictEqual( removed, [] );
        assert.strictEqual( destructive, false );
    } );

    /*
    { "$lookup": { "from": "users", "localField": "userId", "foreignField": "_id", "as": "user" } }
    { "$lookup": { "from": "orders", "let": { "id": "$_id" }, "pipeline": [{ "$match": { "$expr": { "$eq": ["$userId", "$$id"] } } }], "as": "orders" } }
    { "$lookup": { "from": "reviews", "localField": "productId", "foreignField": "product", "as": "reviews" } }
     */
    it( 'should extract fields from $lookup', () =>
    {
        const stage = { "$lookup": { "from": "users", "localField": "userId", "foreignField": "_id", "as": "user" } };
        const { used, produced, destructive, removed } = extractFields( stage );

        assert.deepStrictEqual( used, ['userId'] );
        assert.deepStrictEqual( produced, ['user'] );
        assert.deepStrictEqual( removed, [] );
        assert.strictEqual( destructive, false );
    } );

    it( 'should extract fields from $lookup with pipeline', () =>
    {
        const stage = { "$lookup": {
            "from": "orders",
            "let": { "id": "$_id" },
            "pipeline": [ { "$match": { "$expr": { "$eq": [ "$userId", "$$id" ] } } } ],
            "as": "orders"
        } };
        const { used, produced, destructive, removed } = extractFields( stage );

        assert.deepStrictEqual( used, ['_id', 'userId'] );
        assert.deepStrictEqual( produced, ['orders'] );
        assert.deepStrictEqual( removed, [] );
        assert.strictEqual( destructive, false );
    } );

    it( 'should extract fields from $lookup with array operator', () =>
    {
        const stage = { "$lookup": { "from": "reviews", "localField": "productId", "foreignField": "product", "as": "reviews" } };
        const { used, produced, destructive, removed } = extractFields( stage );

        assert.deepStrictEqual( used, ['productId'] );
        assert.deepStrictEqual( produced, ['reviews'] );
        assert.deepStrictEqual( removed, [] );
        assert.strictEqual( destructive, false );
    } );

    it( 'should extract fields from $unwind', () =>
    {
        const stage = { "$unwind": "$items" };
        const { used, produced, destructive, removed } = extractFields( stage );

        assert.deepStrictEqual( used, ['items'] );
        assert.deepStrictEqual( produced, ['items'] );
        assert.deepStrictEqual( removed, [] );
        assert.strictEqual( destructive, false );
    } );

    it( 'should extract fields from $unwind with options', () =>
    {
        const stage = { "$unwind": { "path": "$tags", "preserveNullAndEmptyArrays": true } };
        const { used, produced, destructive, removed } = extractFields( stage );

        assert.deepStrictEqual( used, ['tags'] );
        assert.deepStrictEqual( produced, ['tags'] );
        assert.deepStrictEqual( removed, [] );
        assert.strictEqual( destructive, false );
    } );

    it( 'should extract fields from $unwind with options and index', () =>
    {
        const stage = { "$unwind": { "path": "$orders", "includeArrayIndex": "orderIndex" } };
        const { used, produced, destructive, removed } = extractFields( stage );

        assert.deepStrictEqual( used, ['orders'] );
        assert.deepStrictEqual( produced, ['orders'] );
        assert.deepStrictEqual( removed, [] );
        assert.strictEqual( destructive, false );
    } );

    /*
    { "$addFields": { "totalPrice": { "$multiply": ["$price", "$quantity"] } } }
    { "$set": { "fullName": { "$concat": ["$firstName", " ", "$lastName"] } } }
    { "$addFields": { "status": { "$cond": [{ "$gte": ["$score", 50] }, "passed", "failed"] } } }
     */
    it( 'should extract fields from $addFields', () =>
    {
        const stage = { "$addFields": { "totalPrice": { "$multiply": ["$price", "$quantity"] } } };
        const { used, produced, destructive, removed } = extractFields( stage );

        assert.deepStrictEqual( used, ['price', 'quantity'] );
        assert.deepStrictEqual( produced, ['totalPrice'] );
        assert.deepStrictEqual( removed, [] );
        assert.strictEqual( destructive, false );
    } );

    it( 'should extract fields from $set', () =>
    {
        const stage = { "$set": { "fullName": { "$concat": ["$firstName", " ", "$lastName"] } } };
        const { used, produced, destructive, removed } = extractFields( stage );

        assert.deepStrictEqual( used, ['firstName', 'lastName'] );
        assert.deepStrictEqual( produced, ['fullName'] );
        assert.deepStrictEqual( removed, [] );
        assert.strictEqual( destructive, false );
    } );

    it( 'should extract fields from $addFields with condition', () =>
    {
        const stage = { "$addFields": { "status": { "$cond": [{ "$gte": ["$score", 50] }, "passed", "failed"] } } };
        const { used, produced, destructive, removed } = extractFields( stage );

        assert.deepStrictEqual( used, ['score'] );
        assert.deepStrictEqual( produced, ['status'] );
        assert.deepStrictEqual( removed, [] );
        assert.strictEqual( destructive, false );
    } );

    it( 'should extract fields from $bucket', () =>
    {
        const stage = { "$bucket": { "groupBy": "$age", "boundaries": [0, 18, 30, 50, 100], "default": "other", "output": { "count": { "$sum": 1 } } } };
        const { used, produced, destructive, removed } = extractFields( stage );

        assert.deepStrictEqual( used, ['age'] );
        assert.deepStrictEqual( produced, ['count'] );
        assert.deepStrictEqual( removed, [] );
        assert.strictEqual( destructive, true );
    } );

    it( 'should extract fields from $bucketAuto', () =>
    {
        const stage = { "$bucketAuto": { "groupBy": "$price", "buckets": 5, "output": { "average": { "$avg": "$price" } } } };
        const { used, produced, destructive, removed } = extractFields( stage );

        assert.deepStrictEqual( used, ['price'] );
        assert.deepStrictEqual( produced, ['average'] );
        assert.deepStrictEqual( removed, [] );
        assert.strictEqual( destructive, true );
    } );

    it( 'should extract fields from $facet', () =>
    {
        const stage = { "$facet": {
            "priceStats": [{ "$group": { "_id": null, "avgPrice": { "$avg": "$price" } } }],
            "categoryCount": [{ "$group": { "_id": "$category", "count": { "$sum": 1 } } }]
        } };
        const { used, produced, destructive, removed } = extractFields( stage );

        assert.deepStrictEqual( used, ['price', 'category'] );
        assert.deepStrictEqual( produced, ['priceStats', 'categoryCount'] );
        assert.deepStrictEqual( removed, [] );
        assert.strictEqual( destructive, true );
    } );

    it( 'should extract fields from $graphLookup', () =>
    {
        const stage = { "$graphLookup": { "from": "employees", "startWith": "$managerId", "connectFromField": "managerId", "connectToField": "_id", "as": "hierarchy" } };
        const { used, produced, destructive, removed } = extractFields( stage );

        assert.deepStrictEqual( used, ['managerId', '_id'] );
        assert.deepStrictEqual( produced, ['hierarchy'] );
        assert.deepStrictEqual( removed, [] );
        assert.strictEqual( destructive, false );
    } );

    /*
    { "$search": { "index": "default", "text": { "query": "mongodb", "path": "description" } } }
    { "$search": { "index": "geoIndex", "geo": { "circle": { "center": { "type": "Point", "coordinates": [-73.97, 40.77] }, "radius": 1000 } } } }
     */
    it( 'should extract fields from $search', () =>
    {
        const stage = { "$search": { "index": "default", "text": { "query": "mongodb", "path": "description" } } };
        const { used, produced, destructive, removed } = extractFields( stage );

        assert.deepStrictEqual( used, [] );
        assert.deepStrictEqual( produced, [] );
        assert.deepStrictEqual( removed, [] );
        assert.strictEqual( destructive, false );
    } );

    it( 'should extract fields from $search with geo', () =>
    {
        const stage = { "$search": { "index": "geoIndex", "geo": { "circle": { "center": { "type": "Point", "coordinates": [-73.97, 40.77] }, "radius": 1000 } } } };
        const { used, produced, destructive, removed } = extractFields( stage );

        assert.deepStrictEqual( used, [] );
        assert.deepStrictEqual( produced, [] );
        assert.deepStrictEqual( removed, [] );
        assert.strictEqual( destructive, false );
    } );

    it( 'should extract fields from $sort', () =>
    {
        const stage = { "$sort": { "price": -1 } };
        const { used, produced, destructive, removed } = extractFields( stage );

        assert.deepStrictEqual( used, ['price'] );
        assert.deepStrictEqual( produced, [] );
        assert.deepStrictEqual( removed, [] );
        assert.strictEqual( destructive, false );
    } );

    it( 'should extract fields from $sort with multiple fields', () =>
    {
        const stage = { "$sort": { "createdAt": 1, "name": -1 } };
        const { used, produced, destructive, removed } = extractFields( stage );

        assert.deepStrictEqual( used, ['createdAt', 'name'] );
        assert.deepStrictEqual( produced, [] );
        assert.deepStrictEqual( removed, [] );
        assert.strictEqual( destructive, false );
    } );

    it( 'should extract fields from $count', () =>
    {
        const stage = { "$count": "totalDocuments" };
        const { used, produced, destructive, removed } = extractFields( stage );

        assert.deepStrictEqual( used, [] );
        assert.deepStrictEqual( produced, ['totalDocuments'] );
        assert.deepStrictEqual( removed, [] );
        assert.strictEqual( destructive, true );
    } );
} );

describe( 'calculateDependencies', () => {
    it( 'should calculate dependencies for a complex pipeline', () => {
        const pipeline = [
            {
                $lookup: {
                    from: 'orders',
                    localField: 'someField',
                    foreignField: 'otherField',
                    as: 'lookupField'
                }
            },
            {
                $match: {
                    status: 'active'
                }
            },
            {
                $lookup: {
                    from: 'users',
                    localField: 'userId',
                    foreignField: '_id',
                    as: 'user'
                }
            },
            { $unwind: '$user' },
            { $match: { name: 'John', 'user.name': 'John' } },
            {
                $lookup: {
                    from: 'orders',
                    localField: 'user.id',
                    foreignField: 'userId',
                    as: 'userOrders'
                }
            },
            {
                $replaceWith: {
                    $mergeObjects: [ { _id: "$_id", first: "", last: "" }, "$name", { orders: "$userOrders" } ]
                }
            },
            { $match: { orders: { $ne: [] } } },
            { $project: { name: { $concat: [ "$first", " ", "$last" ] }, orders: 1, _id: 0 } },
            { $sort: { createdAt: -1 } },
            { $limit: 10 }
        ]

        const res = calculateDependencies( pipeline );

        console.log( res );
    } );
} )

describe( 'optimizePipeline', () => {
    it( 'should not reverse order of operations - $limit, $sort', () => {
        const pipeline = [
            { $limit: 10 },
            { $sort: { createdAt: -1 } },
        ];

        const optimized = optimizePipeline( pipeline );

        assert.deepStrictEqual( optimized, pipeline );
    } );

    it( 'should not reverse order of operations - $limit, $sort', () => {
        const pipeline = [
            { $sort: { createdAt: -1 } },
            { $limit: 10 },
        ];

        const optimized = optimizePipeline( pipeline );

        assert.deepStrictEqual( optimized, pipeline );
    } );

    it( 'should optimize simple pipeline - $lookup', () => {
        const pipeline = [
            {
                $lookup: {
                    from: 'users',
                    localField: 'userId',
                    foreignField: '_id',
                    as: 'user'
                }
            },
            { $sort: { createdAt: -1 } },
            { $limit: 10 },
        ];

        const optimized = optimizePipeline( pipeline );

        const order = [1, 2, 0]
        const expected = order.map( i => pipeline[i] );
        assert.deepStrictEqual( optimized, expected );
    } );

    it( 'should optimize simple pipeline - $lookup', () => {
        const pipeline = [
            {
                $lookup: {
                    from: 'users',
                    localField: 'userId',
                    foreignField: '_id',
                    as: 'user'
                }
            },
            { $limit: 10 },
            { $sort: { createdAt: -1 } },
        ];

        const optimized = optimizePipeline( pipeline );

        const order = [1, 2, 0]
        const expected = order.map( i => pipeline[i] );
        assert.deepStrictEqual( optimized, expected );
    } );

    it( 'should optimize pipeline with $unwind', () => {
        const pipeline = [
            {
                $match: {
                    status: 'active'
                }
            },
            {
                $lookup: {
                    from: 'users',
                    localField: 'userId',
                    foreignField: '_id',
                    as: 'user'
                }
            },
            { $unwind: '$user' },
            { $match: { name: 'John' } },
            {
                $lookup: {
                    from: 'orders',
                    localField: 'user.id',
                    foreignField: 'userId',
                    as: 'userOrders'
                }
            },
            {
                $replaceWith: {
                    $mergeObjects: [ { _id: "$_id", first: "", last: "" }, "$name", { orders: "$userOrders" } ]
                }
            },
            { $match: { orders: { $ne: [] } } },
            { $sort: { createdAt: -1 } },
            { $limit: 10 },
            { $project: { name: { $concat: [ "$first", " ", "$last" ] }, orders: 1, _id: 0 } },
        ];

        const optimized = optimizePipeline( pipeline );

        const order = [3, 0, 1, 2, 4, 5, 6, 9, 7, 8]
        const expected = order.map( i => pipeline[i] );
        assert.deepStrictEqual( optimized, expected );
    });

    it( 'should keep order of operations', () => {
        const pipeline = [
            { $sort: { createdAt: -1 } },
            { $limit: 10 },
            {
                $match: {
                    status: 'active'
                }
            }
        ];

        const optimized = optimizePipeline( pipeline );
        assert.deepStrictEqual( optimized, pipeline );
    });

    it('should handle $set and $unset', () => {
        const pipeline = [
            { $set: { fullName: { $concat: ['$firstName', ' ', '$lastName'] } } },
            { $match: { firstName: 'John Doe' } },
            { $lookup: { from: 'users', localField: 'fullName', foreignField: '_id', as: 'user' } },
            { $unset: ['firstName', 'lastName', 'fullName'] },
            { $sort: { createdAt: -1 } },
            { $limit: 10 }
        ]

        const optimized = optimizePipeline( pipeline );

        const order = [1, 4, 5, 0, 2, 3]
        const expected = order.map( i => pipeline[i] );
        assert.deepStrictEqual( optimized, expected );
    })

    it( 'should optimize complicated query', () => {
        const pipeline = [
            {
                $lookup: {
                    from: "orders",
                    localField: "userId",
                    foreignField: "userId",
                    as: "userOrders"
                }
            },
            { $unwind: "$userOrders" },
            { $match: { "user.age": { $gt: 18 } } },
            {
                $group: {
                    _id: "$userId",
                    totalSpent: { $sum: "$userOrders.amount" }
                }
            },
            {
                $project: {
                    _id: 1,
                    totalSpent: 1,
                    status: { $cond: { if: { $gte: [ "$totalSpent", 1000 ] }, then: "VIP", else: "Regular" } }
                }
            }
        ];

        const optimized = optimizePipeline( pipeline );

        const order = [2, 0, 1, 3, 4]
        const expected = order.map( i => pipeline[i] );
        assert.deepStrictEqual( optimized, expected );
    })
} )


function test()
{
    // test1()
    test2()
    // test3()
}

function test1()
{
    const pipeline = [
        {
            $lookup: {
                from: 'users',
                localField: 'userId',
                foreignField: '_id',
                as: 'user'
            }
        },
        { $sort: { createdAt: -1 } },
        { $limit: 10 }
    ];
    const expected = [
        pipeline[1],
        pipeline[2],
        pipeline[0]
    ]

    const optimized = optimizePipeline( pipeline );

    if ( JSON.stringify(optimized) !== JSON.stringify( expected ) )
    {
        throw new Error( `test1() - Expected \n${JSON.stringify( expected, null, 2 )}, got \n${JSON.stringify( optimized, null, 2 )}` );
    }
}

function test2()
{
    const replaceWith = [
        { $replaceWith: "$name" },
        { $replaceWith: { id: '$_id', name: '$name.first' } },
        { $replaceWith: { $ifNull: [ "$name", { _id: "$_id", missingName: true} ] } },
        { $replaceWith: { $mergeObjects: [ { _id: "$_id", first: "", last: "" }, "$name" ] } },
        { $replaceWith: { $arrayToObject: "$items" } },
    ]
    const results = [
        { used: ['name'], produced: [] },
        { used: ['_id', 'name.first'], produced: ['id', 'name'] },
        { used: ['name', '_id'], produced: ['_id', 'missingName'] },
        { used: ['_id', 'name'], produced: ['_id', 'first', 'last'] },
        { used: ['items'], produced: [/* TODO: to čo je vnutri items - ani boh nezna */] }
    ]

    for ( const stage of replaceWith )
    {
        const { used, produced } = extractFields( stage );

        console.log( used, produced );
    }
}


const x = [
    { $lookup: {
            from: 'users',
            localField: 'userID',
            foreignField: '_id',
            as: 'user'
        }
    }
]




const y = [
    { $match: { "user.age": { $gt: 18 } } },
    {
        $lookup: {
            from: "orders",
            localField: "userId",
            foreignField: "userId",
            as: "userOrders"
        }
    },
    { $unwind: "$userOrders" },
    {
        $group: {
            _id: "$userId",
            totalSpent: { $sum: "$userOrders.amount" }
        }
    },
    {
        $project: {
            _id: 1,
            totalSpent: 1,
            status: { $cond: { if: { $gte: [ "$totalSpent", 1000 ] }, then: "VIP", else: "Regular" } }
        }
    }
];

/*
fields:         uses:               produces:
$match          user.age      ->    -
$lookup         userId        ->    userOrders
$unwind         userOrders    ->    -
$group          userId        ->    totalSpent
                $userOrders.amount  -
$project        _id           ->    status
                totalSpent    ->    status
                totalSpent    ->    -
                -             ->    status
 */



