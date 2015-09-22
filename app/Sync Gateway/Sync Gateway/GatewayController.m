//
//  GatewayController.m
//  Sync Gateway
//
//  Created by Jens Alfke on 8/29/15.
//  Copyright © 2015 Couchbase. All rights reserved.
//

#import "GatewayController.h"


#define kLogFileName @"sync_gateway.log"
#define kConfigFileName @"config.json"

#define kStartupTimeout 5.0

//#define SHOW_ALL_LOGS


@interface GatewayController ()
@property (readwrite) BOOL isRunning;
@property (readwrite) NSString* adminPort;
@property (readwrite) NSString* apiPort;
@property (readwrite) NSString* errorMessage;
@end


@implementation GatewayController
{
    NSTask *_task;
    NSFileHandle* _taskOutput;
    FILE *_logFD;
    NSMutableData* _logLine;
    NSTimer* _startupTimer;
    BOOL _startupTimedOut;
}

@synthesize launchPath=_launchPath, dataDirectory=_dataDirectory, configFile=_configFile;
@synthesize adminPort=_adminPort, apiPort=_apiPort, onStarted=_onStarted, onStopped=_onStopped;


- (instancetype) init {
    self = [super init];
    if (self) {
        _launchPath = [[NSBundle mainBundle] pathForResource: @"sync_gateway" ofType: @""];

        NSFileManager* fmgr = [NSFileManager defaultManager];
        NSURL* appSupport = [fmgr URLForDirectory: NSApplicationSupportDirectory
                                         inDomain: NSUserDomainMask
                                appropriateForURL: nil
                                           create: YES
                                            error: NULL];
        _dataDirectory = [appSupport.path stringByAppendingPathComponent: @"SyncGateway"];
        [[NSFileManager defaultManager] createDirectoryAtPath: _dataDirectory
                                  withIntermediateDirectories: NO attributes: nil error: NULL];

        _configFile = [_dataDirectory stringByAppendingPathComponent: kConfigFileName];
        if (![fmgr fileExistsAtPath: _configFile isDirectory: NULL]) {
            NSString* tmpl = [[NSBundle mainBundle] pathForResource: @"config" ofType: @"json"];
            [fmgr copyItemAtPath: tmpl toPath: _configFile error: NULL];
        }
    }
    return self;
}


- (void) start {
    _task = [[NSTask alloc] init];

    pid_t group = setsid();
    if (group == -1) {
        NSLog(@"setsid() == -1, errno = %d", errno);
        group = getpgrp();
    }
    NSLog(@"group = %d", group);

    [_task setCurrentDirectoryPath: _dataDirectory];
    _task.launchPath = _launchPath;

    __weak GatewayController* weakSelf = self;
    _task.terminationHandler = ^(NSTask* task) {
        dispatch_async(dispatch_get_main_queue(), ^{
            [weakSelf stopped];
        });
    };

    NSPipe* out = [[NSPipe alloc] init];
    _task.standardOutput = out;
    _task.standardError = out;
    _taskOutput = out.fileHandleForReading;
    _taskOutput.readabilityHandler = ^(NSFileHandle* fh) {
        NSData* output = fh.availableData;
        dispatch_async(dispatch_get_main_queue(), ^{
            [weakSelf readTaskOutput: output];
        });
    };

    NSMutableArray* args = [NSMutableArray new];
    if (_loggingOptions.count > 0)
        [args addObject: [NSString stringWithFormat: @"-log=%@",
                                [_loggingOptions componentsJoinedByString: @","]]];
    [args addObject: _configFile];
    _task.arguments = args;

    _logFD = fopen(self.logFile.fileSystemRepresentation, "a");
    [self logString: [NSString stringWithFormat: @"\n\n-------- Launched at %@\n", [NSDate date]]];

    _startupTimer = [NSTimer scheduledTimerWithTimeInterval: kStartupTimeout
                                                     target: self
                                                   selector: @selector(startupTimedOut)
                                                   userInfo: nil repeats: NO];

    [_task launch];
    NSLog(@"GATEWAY: Launched -- pid = %d", _task.processIdentifier);
}


- (void) startupTimedOut {
    NSLog(@"GATEWAY: Failed to start up in %g sec; terminating", kStartupTimeout);
    _startupTimedOut = YES;
    [_task terminate];
}


- (void) started {
    NSLog(@"GATEWAY: Started!");
    [_startupTimer invalidate];
    _startupTimer = nil;
    self.isRunning = YES;
    if (_onStarted)
        _onStarted();
}


- (void) stop {
    NSLog(@"GATEWAY: Stopping...");
    [_task interrupt];
}


- (void) stopped {
    [_startupTimer invalidate];
    _startupTimer = nil;

    NSTaskTerminationReason reason = _task.terminationReason;
    if (_startupTimedOut)
        reason = kGatewayTerminationReasonTimedOut;
    
    NSLog(@"GATEWAY: Stopped! reason=%ld, status=%d",
          (long)reason, _task.terminationStatus);
    if (_onStopped)
        _onStopped(reason, _task.terminationStatus);
    _task = nil;
    if (_logFD) {
        fclose(_logFD);
        _logFD = NULL;
    }
}


#pragma mark - LOGGING:


- (NSString*) logFile {
    return [_dataDirectory stringByAppendingPathComponent: kLogFileName];
}


- (void) readTaskOutput: (NSData*)output {
    [self logData: output];

    const char* start = output.bytes, *end = start + output.length;
    while (start < end) {
        const char* eol = memchr(start, '\n', end-start);
        if (eol == NULL)
            eol = end;

        const char* msgStart = start;
        while (msgStart < eol && !isspace(*msgStart))
            ++msgStart;
        if (msgStart < eol)
            ++msgStart;
        else
            msgStart = start;
        NSString* line = [[NSString alloc] initWithBytes: msgStart length: eol-msgStart
                                                encoding: NSUTF8StringEncoding];
        [self loggedLine: line];
        start = eol + 1;
    }
}


- (void) loggedLine: (NSString*)line {
#ifdef SHOW_ALL_LOGS
    NSLog(@"GATEWAY: Logged >%@<", line);
#endif
    if ([line hasPrefix: @"FATAL: "])
        self.errorMessage = [line substringFromIndex: 7];
    else if ([line hasPrefix: @"Starting admin server on "])
        self.adminPort = [line substringFromIndex: 25];
    else if ([line hasPrefix: @"Starting server on "])
        self.apiPort = [line substringFromIndex: 19];

    if (!_isRunning && self.adminPort && self.apiPort && !self.errorMessage)
        [self started];
}


- (void) logData: (NSData*)data {
    if (_logFD && data)
        fwrite(data.bytes, data.length, 1, _logFD);
}

- (void) logString: (NSString*)str {
    if (str)
        [self logData: [str dataUsingEncoding: NSUTF8StringEncoding]];
}


@end
