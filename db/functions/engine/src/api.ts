/*
Copyright 2022-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

import { Args, User, Config, Database, Context, Credentials, Document, JSONObject } from './types'
import { ClearCallDepth, MakeDatabase, Upstream } from "./impl";


/** The interface the native code needs to implement.
 */
export interface NativeAPI {
    query(fnName: string,
          n1ql: string,
          argsJSON: string | undefined,
          asAdmin: boolean) : string;
    get(docID: string,
        collection: string,
        asAdmin: boolean) : string | null;
    save(docJSON: string,
         docID: string | undefined,
         collection: string,
         asAdmin: boolean) : string | null;
    delete(docID: string,
           revID: string | undefined,
           collection: string,
           asAdmin: boolean) : boolean;
}


// Wraps a `NativeAPI` and exposes it as an Upstream for a Database to use
class UpstreamNativeImpl implements Upstream {
    constructor(private native: NativeAPI) { }

    query(fnName: string, n1ql: string, args: Args | undefined, user: User) : JSONObject[] {
        let result = this.native.query(fnName, n1ql, this.stringify(args), user.isAdmin);
        return JSON.parse(result);
    }

    get(docID: string, collection: string, user: User) : Document | null {
        let jresult = this.native.get(docID, collection, user.isAdmin);
        if (jresult === null) return jresult;
        return this.parseDoc(jresult)
    }

    save(doc: object, docID: string | undefined, collection: string, user: User) : string | null {
        return this.native.save(JSON.stringify(doc), docID, collection, user.isAdmin);
    }

    delete(docID: string, revID: string | undefined, collection: string, user: User) : boolean {
        return this.native.delete(docID, revID, collection, user.isAdmin);
    }

    private stringify(obj: object | undefined) : string | undefined {
        return obj ? JSON.stringify(obj) : undefined;
    }

    private parseDoc(json: string) : Document {
        let result = JSON.parse(json)
        if (typeof(result) !== "object")
            throw Error("NativeAPI returned JSON that's not an Object");
        return result as Document
    }
}


/** The API this module implements, and the native code (evaluator.go) calls. */
export class API {
    /** Constructs an instance and parses the configuration.
     *  Should not throw exceptions, but sets the `errors` property if config is invalid.
     */
    constructor(configJSON: string, native: NativeAPI) {
        let config = JSON.parse(configJSON) as Config;
        let [db, errors] = MakeDatabase(config.functions, config.graphql,
                                        new UpstreamNativeImpl(native));
        if (db !== null)  this.db = db;
        this.errors = errors;
    }

    /** Configuration errors. If there are errors, the API must not be called. */
    readonly errors: string[] | null;

    /** Calls a named function. */
    callFunction(name: string,
                 argsJSON: string | undefined,
                 user: string | undefined,
                 roles: string | undefined,
                 channels: string | undefined,
                 mutationAllowed: boolean) : string | Promise<string>
    {
        let args = argsJSON ? JSON.parse(argsJSON) : undefined;
        let context = this.makeContext(user, roles, channels, mutationAllowed);
        ClearCallDepth();
        let result = this.db.callFunction(context, name, args);
        if (result instanceof Promise) {
            return result.then( result => JSON.stringify(result) );
        } else {
            return JSON.stringify(result);
        }
    }

    /** Runs a GraphQL query. */
    graphql(query: string,
            operationName: string | undefined,
            variablesJSON: string | undefined,
            user: string | undefined,
            roles: string | undefined,
            channels: string | undefined,
            mutationAllowed: boolean) : Promise<string>
    {
        if (operationName === "") operationName = undefined;
        let vars = variablesJSON ? JSON.parse(variablesJSON) : undefined;
        let context = this.makeContext(user, roles, channels, mutationAllowed);
        ClearCallDepth();
        return this.db.graphql(context, query, vars, operationName)
            .then( result => JSON.stringify(result) );
    }

    private makeContext(user: string | undefined,
                        roles: string | undefined,
                        channels: string | undefined,
                        mutationAllowed: boolean) : Context
    {
        var credentials: Credentials | null = null;
        if (user !== undefined) {
            credentials = [user,
                           roles?.split(',') ?? [],
                           channels?.split(',') ?? []];
        }
        return this.db.makeContext(credentials, mutationAllowed)
    }

    private db!: Database;
};


/** Main entry point, called by Go `NewEvaluator()`. */
export function main(configJSON: string, native: NativeAPI) : API {
    return new API(configJSON, native);
}
