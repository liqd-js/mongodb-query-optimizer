type ExtractedFields = { used: string[], produced: string[], removed: string[], destructive: boolean }
type FieldDependency = ExtractedFields & { stage: any, stageID: number }
type Stage = { stageID: number, stage: any, destructive: boolean, dependencies: any[], dependents: any[] }
type Stage2 = { stageID: number, stage: any, destructive: boolean, dependents: any[] }

const STAGE_DEPS: {[stage: string]: string[]}  = {
    '$limit': ['$sort', '$skip', '$sample', '$match'],
    '$sort': ['$limit', '$skip', '$sample'],
    '$skip': ['$sort', '$limit', '$sample', '$match'],
    '$sample': ['$sort', '$limit', 'skip', '$match'],
    '$match': ['$limit', '$skip', '$sample', '$group', '$bucket', '$bucketAuto', '$facet'],
}
// const ADDITIVE_STAGES = [
//     '$addFields',
//     '$set',
//     '$unset',
//     '$lookup',
//     '$graphLookup',
// ]
const STAGE_ORDER = [
    '$match',
    '$sortByCount',
    '$unwind',
    '$project',
    '$search',
    '$sort',
    '$limit',
    '$skip',
    '$addFields',
    '$set',
    '$lookup',
    '$graphLookup',
    '$group',
    '$bucket',
    '$bucketAuto',
    '$facet',
    '$count',
    '$unset',
    '$sample',
]

/**
 * Optimize the pipeline:
 *  - remove unnecessary stages
 *  - reorder stages, move $lookup to the end if possible
 *  - remove stages that don't affect the count
 * @param pipeline
 */
export function optimizePipeline( pipeline: any[] ): any[]
{
    /*
    rules:
        - keep sort, limit, skip, sample order
        - match, project, group should be as early as possible
        - lookup should be as late as possible
     */
    const dependencies = calculateDependencies( pipeline );

    const sorted = topSort( dependencies );

    return sorted.map( ({ stage }) => stage );
}

export function calculateDependencies( pipeline: any[] ): Stage[]
{
    const fieldDependencies: FieldDependency[] = [];

    let i = 0;
    for ( const stage of pipeline )
    {
        const extracted = extractFields( stage );

        fieldDependencies.push({ stageID: i++, stage, ...extracted });

        // TODO: $project, $group - destructive operators - vyhodia ostatné fieldy, všetko, čo používa aj niečo iné okrem toho, čo produkujú, musí byť pred nimi
    }

    const dependencies: Stage[] = []

    for ( let i = 0; i < fieldDependencies.length; ++i )
    {
        const { stageID, stage, destructive } = fieldDependencies[i];

        let deps = fieldDependencies
            .slice(0, i);

        const destructiveIndex = deps.slice().reverse().findIndex( el => el.destructive );
        if ( destructiveIndex !== -1 )
        {
            const destructiveDeps = deps.slice(0, deps.length - destructiveIndex);
            const allDeps = deps.filter( dep => filterDependent( dep, fieldDependencies[i] ) );
            deps = [...destructiveDeps, ...allDeps].reduce( (acc, dep) => {
                if ( !acc.find( d => d.stageID === dep.stageID ) )
                {
                    acc.push( dep );
                }
                return acc;
            }, [] as FieldDependency[]);
        }
        else
        {
            deps = deps.filter( dep => filterDependent( dep, fieldDependencies[i] ) );
        }

        dependencies.push({ stageID, stage, destructive, dependencies: deps, dependents: [] });
    }

    for ( let i = 0; i < fieldDependencies.length; ++i )
    {
        const deps = dependencies[i].dependencies;

        for ( const dep of deps )
        {
            const dependency = dependencies.find( d => d.stageID === dep.stageID );

            if ( dependency )
            {
                dependency.dependents.push( dependencies[i] );
            }
        }
    }

    return dependencies;
}

function topSort( graph: Stage2[] )
{
    let queue: Stage2[] = [];
    const outDegrees: {[stage: string]: number} = {};

    for ( const { stageID } of graph )
    {
        outDegrees[stageID] = 0;
    }

    for ( const { dependents } of graph )
    {
        for ( const { stageID } of dependents )
        {
            outDegrees[stageID]++;
        }
    }

    const initialWave: Stage2[] = [];
    for ( const { stageID, stage, destructive, dependents } of graph )
    {
        if ( outDegrees[stageID] === 0 )
        {
            initialWave.push({ stageID, stage, destructive, dependents });
        }
    }
    initialWave.sort( stageSort );
    queue.push(...initialWave);

    const result: Stage2[] = [];

    while ( queue.length > 0 )
    {
        const { stageID, stage, destructive, dependents } = queue.shift() as Stage;
        result.push({ stageID, stage, destructive, dependents });

        const wave: Stage2[] = [];
        const subQueue: Stage2[] = [];

        for ( const dep of dependents )
        {
            outDegrees[dep.stageID]--;
            if ( [...queue, ...result].find( r => r.stageID === dep.stageID ) )
            {
                continue;
            }

            if ( outDegrees[dep.stageID] === 0 )
                // || ( outDegrees[dep.stageID] === 1 && !dep.destructive ) )
            {
                wave.push(dep);
            }
        }

        wave.sort( stageSort );
        subQueue.push(...wave);

        // queue.push(...wave);
        queue = [...subQueue, ...queue];
    }

    return result;
}

function stageSort( a: Stage2, b: Stage2 )
{
    if ( a.destructive && !b.destructive ) { return -1; }
    if ( !a.destructive && b.destructive ) { return 1; }

    const aIndex = STAGE_ORDER.indexOf( Object.keys(a.stage)[0] );
    const bIndex = STAGE_ORDER.indexOf( Object.keys(b.stage)[0] );

    if ( !aIndex ) { return -1; }
    if ( !bIndex ) { return 1; }

    return aIndex - bIndex;
}

/**
 * Filter stages that depend on reference
 * @param dep - potential dependent stage
 * @param reference - reference stage
 */
function filterDependent( dep: FieldDependency, reference: FieldDependency ): boolean
{
    if ( reference.destructive )
    {
        return true;
    }

    const refStageKey = Object.keys(reference.stage)[0];
    const depStageKey = Object.keys(dep.stage)[0];

    if ( STAGE_DEPS[refStageKey] && STAGE_DEPS[refStageKey].includes( depStageKey ) )
    {
        return true;
    }

    // TODO: prefix match
    return dep.produced.some( field => reference.used.some( used => field === used || field.split('.')[0] === used.split('.')[0] ) );
}

export function extractFields( stage: any ): ExtractedFields
{
    const usedFields: Set<string> = new Set();
    const producedFields: Set<string> = new Set();
    const removedFields: Set<string> = new Set();
    let destructive = false;

    const operator = Object.keys(stage)[0];

    switch ( operator )
    {
        case '$match':
            const extracted = extractRecursively( stage.$match, true );
            for ( const field of extracted )
            {
                usedFields.add(field);
            }
            break;

        case '$sortByCount':
            if ( typeof stage.$sortByCount === 'string' )
            {
                usedFields.add(stage.$sortByCount);
            }
            else
            {
                extractRecursively( stage.$sortByCount, false ).forEach(key => usedFields.add(key));
            }
            break;

        case '$project':
        case '$group':
            // TODO: destructive operators - vyhodia ostatné fieldy, všetko, čo používa aj niečo iné okrem toho, čo produkujú, musí byť pred nimi
            for ( const [key, value] of Object.entries(stage[operator]) )
            {
                if ( typeof value === 'object' || (typeof value === 'string' && value.startsWith('$')) )
                {
                    extractRecursively( value, false ).forEach(key => usedFields.add(key));
                }

                if ( value === 0 )
                {
                    removedFields.add( key );
                }
                else if ( value === 1 )
                {
                    usedFields.add( key );
                    producedFields.add( key );
                }
                else
                {
                    producedFields.add( key );
                }
            }
            break;

        case '$addFields':
        case '$set':
            for ( const [key, value] of Object.entries(stage[operator]) )
            {
                if ( typeof value === 'object' || (typeof value === 'string' && value.startsWith('$')) )
                {
                    extractRecursively( value, false ).forEach(key => usedFields.add(key));
                }

                producedFields.add(key);
            }
            break;

        case '$unset':
            const unset = Array.isArray( stage.$unset ) ? stage.$unset : [stage.$unset];
            for ( const field of unset )
            {
                usedFields.add( field );
                removedFields.add( field );
            }
            break;

        case '$lookup':
            if ( stage.$lookup.localField )
            {
                usedFields.add( stage.$lookup.localField )
            }
            if ( stage.$lookup.let )
            {
                for ( const [key, value] of Object.entries(stage.$lookup.let) )
                {
                    if ( typeof value === 'string' && value.startsWith('$') )
                    {
                        usedFields.add(value.replace('$', ''));
                    }
                    else if ( typeof value === 'object' )
                    {
                        extractRecursively( value, false ).forEach(key => usedFields.add(key));
                    }
                }
            }
            if ( stage.$lookup.pipeline )
            {
                for ( const s of stage.$lookup.pipeline )
                {
                    const { used } = extractFields( s );
                    used.forEach( key => usedFields.add(key) );
                }
            }
            producedFields.add( stage.$lookup.as );
            break;

        case '$bucket':
        case '$bucketAuto':
            if ( typeof stage[operator].groupBy !== 'string' )
            {
                throw new Error(`Unsupported $bucket groupBy type: "${typeof stage[operator].groupBy}"`);
            }
            usedFields.add( stage[operator].groupBy.replace('$', '') );

            for ( const [key, value] of Object.entries(stage[operator].output) )
            {
                producedFields.add( key );

                if ( typeof value === 'object' || (typeof value === 'string' && value.startsWith('$')) )
                {
                    extractRecursively( value, false ).forEach(key => usedFields.add(key));
                }
            }

            destructive = true;

            break;

        case '$unwind':
            if ( typeof stage.$unwind === 'object' )
            {
                usedFields.add(stage.$unwind.path.replace('$', ''));
                producedFields.add(stage.$unwind.path.replace('$', ''));
            }
            else
            {
                usedFields.add(stage.$unwind.replace('$', ''));
                producedFields.add(stage.$unwind.replace('$', ''));
            }
            break;

        case '$facet':
            for ( const [key, value] of Object.entries(stage.$facet) )
            {
                producedFields.add(key);

                for ( const stage of value as any[] )
                {
                    const { used } = extractFields( stage );
                    used.forEach( key => usedFields.add(key) );
                }
            }
            destructive = true;
            break;

        case '$graphLookup':
            usedFields.add(stage.$graphLookup.startWith.replace('$', ''));
            usedFields.add(stage.$graphLookup.connectFromField.replace('$', ''));
            usedFields.add(stage.$graphLookup.connectToField.replace('$', ''));
            producedFields.add( stage.$graphLookup.as );
            break;

        case '$count':
            producedFields.add( stage.$count );
            destructive = true;
            break;

        case '$search':
        case '$limit':
        case '$skip':
        case '$sample':
            break;

        case '$sort':
            for ( const [key, value] of Object.entries(stage.$sort) )
            {
                usedFields.add(key);
            }
            break;

        case '$replaceWith':
            destructive = true;
            break;

        default:
            throw new Error(`Unsupported stage: "${operator}"`);
    }

    return { used: Array.from(usedFields), produced: Array.from(producedFields), removed: Array.from( removedFields ), destructive }
}

const DATE_OPERATORS = [ '$dayOfYear', '$dayOfMonth', '$dayOfWeek', '$year', '$month', '$week', '$hour', '$minute', '$second', '$millisecond', '$dateToString' ];
const MATHEMATICAL_OPERATORS = [ '$sum', '$subtract', '$multiply', '$divide', '$mod', '$abs', '$ceil', '$floor', '$ln', '$log', '$log10', '$pow', '$sqrt', '$trunc', '$exp', '$round', '$sin', '$cos', '$tan', '$asin', '$acos', '$atan', '$atan2', '$degreesToRadians', '$radiansToDegrees', '$avg', '$min', '$max', '$gt', '$gte', '$lt', '$lte', '$eq', '$ne' ];

/**
 * Extract fields from the object recursively
 * @param obj
 * @param extractKeys - extract keys as well
 *   true - { $match: { year: { $year: '$date' } } } - extract 'year' as well
 *   false - { "$group": { "_id": { "year": { "$year": "$date" } }, "count": { "$sum": 1 } } } - extract 'date' only
 */
function extractRecursively( obj: any, extractKeys: boolean ): Set<string>
{
    const fields: Set<string> = new Set();

    if ( !obj ) { return fields; }

    if ( typeof obj !== 'object' )
    {
        if ( typeof obj === 'string' && obj.startsWith('$') )
        {
            fields.add(obj);
        }
    }
    else if ( Array.isArray(obj) )
    {
        obj.forEach( (item: any) => typeof item === 'string' && item.startsWith('$') && fields.add(item) );
    }
    else
    {
        for ( const [key, value] of Object.entries(obj) )
        {
            if ( key === '$and' || key === '$or' )
            {
                for ( const item of value as any[] )
                {
                    extractRecursively( item, extractKeys ).forEach(key => fields.add(key));
                }
            }
            else if ( key === '$exists' )
            {
                // ignore
            }
            else if ( key === '$expr' )
            {
                extractRecursively( value, extractKeys ).forEach(key => fields.add(key));
            }
            else if ( key === '$map' || key === '$filter' )
            {
                fields.add((value as any).input);
            }
            else if ( key === '$mergeObjects' )
            {
                for ( const item of value as any[] )
                {
                    if ( typeof item === 'string' && item.startsWith('$') )
                    {
                        extractRecursively( item, extractKeys ).forEach(key => fields.add(key));
                    }
                }
            }
            else if ( key === '$cond' )
            {
                extractRecursively( (value as any).if, extractKeys ).forEach(key => fields.add(key));
            }
            else if ( key === '$arrayElemAt' )
            {
                fields.add((value as any[])[0]);
            }
            else if ( key === '$function' )
            {
                (value as any).args
                    .filter( (arg: any) => typeof arg === 'string' && arg.startsWith('$'))
                    .forEach( (arg: string) => fields.add(arg));
            }
            else if ( key === '$switch' )
            {
                for ( const branch of (value as any).branches )
                {
                    extractRecursively( branch.case, extractKeys ).forEach(key => fields.add(key));
                }
                extractRecursively( (value as any).default, extractKeys ).forEach(key => fields.add(key));
            }
            else if ( MATHEMATICAL_OPERATORS.includes(key) )
            {
                extractRecursively( value, extractKeys ).forEach(key => fields.add(key));
            }
            else if ( DATE_OPERATORS.includes(key) )
            {
                const date = typeof value === 'string' ? value : (value as any).date;
                if ( date && date.startsWith('$') )
                {
                    fields.add( date );
                }
                else if ( typeof date === 'object' )
                {
                    extractRecursively( date, extractKeys ).forEach(key => fields.add(key));
                }
            }
            else if ( ['$size', '$push'].includes(key) )
            {
                if ( typeof value === 'string' )
                {
                    fields.add( value );
                }
                else if ( value && typeof value === 'object' && Object.keys( value ).length === 1 && Object.keys( value )[0].startsWith('$') )
                {
                    extractRecursively( value, extractKeys ).forEach(key => fields.add(key));
                }
            }
            else if ( Array.isArray( value ) )
            {
                value.forEach( (item: any) => {
                    if ( typeof item === 'string' && item.startsWith('$') )
                    {
                        fields.add(item);
                    }
                    else if ( typeof item === 'object' )
                    {
                        extractRecursively( item, extractKeys ).forEach( key => fields.add(key) );
                    }
                } );
            }
            else if ( !key.startsWith('$') && extractKeys )
            {
                fields.add(key);
                if ( typeof value === 'object' )
                {
                    extractRecursively( value, extractKeys ).forEach(key => fields.add(key));
                }
            }
            else if ( typeof value === 'object' )
            {
                extractRecursively( value, extractKeys ).forEach(key => fields.add(key));
            }
            else if ( ['$literal'].includes(key) )
            {
                // ignore
            }
            else
            {
                throw new Error(`Unsupported operator: "${key}"`);
            }
        }
    }

    const result: Set<string> = new Set();
    for ( const field of fields )
    {
        result.add(field.startsWith('$') ? field.replace(/^\$/, '') : field);
    }

    return new Set([...result].filter( field => !field.startsWith('$') ));
}