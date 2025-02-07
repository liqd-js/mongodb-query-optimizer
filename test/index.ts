import { extractFields, optimizePipeline } from "../src";

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