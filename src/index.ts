/**
 * Optimize the pipeline:
 *  - remove unnecessary stages
 *  - reorder stages, move $lookup to the end if possible
 *  - remove stages that don't affect the count
 * @param pipeline
 */
export function optimizePipeline( pipeline: any[] ): any[]
{
    for ( const stage of pipeline )
    {
        const { used, produced } = extractFields( stage );

        console.log( stage, { used, produced } );
    }

    return [];
}

export function extractFields( stage: any ): { used: string[], produced: string[] }
{
    const usedFields: Set<string> = new Set();
    const producedFields: Set<string> = new Set();

    const operator = Object.keys(stage)[0];

    switch ( operator )
    {
        case '$match':
            const extracted = extractRecursively( stage.$match );
            for ( const field of extracted )
            {
                usedFields.add(field);
            }
            break;
        case '$project':
        case '$addFields':
        case '$set':
        case '$group':
            for ( const [key, value] of Object.entries(stage[operator]) )
            {
                if ( typeof value === 'object' || (typeof value === 'string' && value.startsWith('$')) )
                {
                    extractRecursively( value ).forEach(key => usedFields.add(key));
                }
            }
            break;
        case '$lookup':
            usedFields.add( stage.$lookup.localField )
            producedFields.add( stage.$lookup.as );
            break;
        // case '$replaceWith':
        //     if ( typeof stage.$replaceWith === 'object' )
        //     {
        //         if ( Object.keys(stage.$replaceWith).length > 1 )       // directly listed fields
        //         {
        //             Object.entries(stage.$replaceWith).forEach(
        //                 ([key, value]) => {
        //                     extractRecursively( value ).forEach(key => usedFields.add(key));
        //                     producedFields.add(key);
        //                 }
        //             );
        //         }
        //         else if ( Object.keys( stage.$replaceWith ).length === 1 && Object.keys( stage.$replaceWith )[0].startsWith('$') )
        //         {
        //             extractRecursively( stage.$replaceWith ).forEach(key => usedFields.add(key));
        //         }
        //         // Object.entries(stage.$replaceWith).forEach(
        //         //     ([key, value]) =>
        //         //         ignoredFields.add(key)
        //         //         && extractRecursively( value ).forEach(key => usedFields.add(key))
        //         // );
        //     }
        //     else
        //     {
        //         usedFields.add(stage.$replaceWith.replace('$', ''));
        //     }
        //     break;
        case '$unwind':
            if ( typeof stage.$unwind === 'object' )
            {
                usedFields.add(stage.$unwind.path.replace('$', ''));
            }
            else
            {
                usedFields.add(stage.$unwind.replace('$', ''));
            }
            break;
        default:
            throw new Error(`Unsupported stage: "${operator}"`);
    }

    return { used: Array.from(usedFields), produced: Array.from(producedFields) }
}

const MATHEMATICAL_OPERATORS = ['$sum', '$subtract', '$multiply', '$divide', '$mod', '$abs', '$ceil', '$floor', '$ln', '$log', '$log10', '$pow', '$sqrt', '$trunc'];
function extractRecursively( obj: any ): Set<string>
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
    else
    {
        for ( const [key, value] of Object.entries(obj) )
        {
            if ( key === '$and' || key === '$or' )
            {
                for ( const item of value as any[] )
                {
                    extractRecursively( item ).forEach(key => fields.add(key));
                }
            }
            else if ( key === '$expr' )
            {
                extractRecursively( value ).forEach(key => fields.add(key));
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
                        extractRecursively( item ).forEach(key => fields.add(key));
                    }
                }
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
                    extractRecursively( branch.case ).forEach(key => fields.add(key));
                }
                extractRecursively( (value as any).default ).forEach(key => fields.add(key));
            }
            else if ( MATHEMATICAL_OPERATORS.includes(key) )
            {
                extractRecursively( value ).forEach(key => fields.add(key));
            }
            else if ( ['$size'].includes(key) )
            {
                if ( typeof value === 'string' )
                {
                    fields.add( value );
                }
                else if ( value && typeof value === 'object' && Object.keys( value ).length === 1 && Object.keys( value )[0].startsWith('$') )
                {
                    extractRecursively( value ).forEach(key => fields.add(key));
                }
            }
            else if ( Array.isArray( value ) )
            {
                value.forEach( (item: any) => typeof item === 'string' && item.startsWith('$') && fields.add(item) );
            }
            else if ( !key.startsWith('$') )
            {
                fields.add(key);
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

    return result;
}