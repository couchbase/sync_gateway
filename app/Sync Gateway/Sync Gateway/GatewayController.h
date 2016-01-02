//
//  GatewayController.h
//  Sync Gateway
//
//  Created by Jens Alfke on 8/29/15.
//  Copyright © 2015 Couchbase. All rights reserved.
//

#import <Foundation/Foundation.h>


// Extension of NSTaskTerminationReason
enum {
    kGatewayTerminationReasonTimedOut = 99
};


/** Programmatic interface to running a Sync Gateway instance locally. */
@interface GatewayController : NSObject

@property (readonly) NSString* launchPath;
@property (readonly) NSString* dataDirectory;
@property (readonly) NSString* configFile;
@property (readonly) NSString* logFile;

@property (copy) NSArray* loggingOptions;

@property (copy) void (^onStarted)();
@property (copy) void (^onStopped)(NSTaskTerminationReason reason, int status);

- (void) start;

- (void) stop;

@property (readonly) BOOL isRunning;

@property (readonly) NSString* adminPort;
@property (readonly) NSString* apiPort;

@property (readonly) NSString* errorMessage;

@end
