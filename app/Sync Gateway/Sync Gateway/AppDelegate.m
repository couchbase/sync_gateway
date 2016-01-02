//
//  AppDelegate.m
//  Sync Gateway
//
//  Created by Jens Alfke on 9/23/13.
//
//

#import "AppDelegate.h"
#import "GatewayController.h"
#import "Terminal.h"


@implementation AppDelegate
{
    BOOL _loggingToFile;
    NSStatusItem* _statusItem;
    GatewayController* _gateway;
}


- (void)applicationDidFinishLaunching:(NSNotification *)aNotification
{
    _statusItem=[[NSStatusBar systemStatusBar] statusItemWithLength: NSSquareStatusItemLength];
    NSImage *statusIcon = [NSImage imageNamed:@"AppIcon.png"];
    statusIcon.size = NSMakeSize(16,16);
    _statusItem.image = statusIcon;
    _statusItem.menu = _statusMenu;
    _statusItem.enabled = YES;
    _statusItem.highlightMode = YES;

    _gateway = [GatewayController new];
    [self startGateway];
}


- (void) applicationWillTerminate:(NSNotification *)notification {
    NSLog(@"Quitting Sync Gateway.app");
    [_gateway stop];
}


- (void) startGateway {
    NSLog(@"Starting sync_gateway ...");
    __weak AppDelegate* weakSelf = self;
    _gateway.onStarted = ^() {
        [weakSelf gatewayStarted];
    };
    _gateway.onStopped = ^(NSTaskTerminationReason reason, int status) {
        [weakSelf gatewayStoppedWithReason: reason status: status];
    };
    [_gateway start];
}


- (void) gatewayStarted {
}


- (void) gatewayStoppedWithReason: (NSTaskTerminationReason)reason status: (int)status {
    switch ((int)reason) {
        case NSTaskTerminationReasonExit:
            if (status != 0) {
                if (_gateway.errorMessage) {
                    [self fatalErrorWithHeadline: @"Sync Gateway Fatal Error"
                                            text: _gateway.errorMessage];
                } else {
                    [self fatalErrorWithHeadline: @"Sync Gateway Exited"
                                            text: [NSString stringWithFormat: @"Status %d", status]];
                }
            }
            break;
        case NSTaskTerminationReasonUncaughtSignal:
            [self fatalErrorWithHeadline: @"Sync Gateway Crashed"
                                    text: [NSString stringWithFormat: @"Status %d", status]];
            break;
        case kGatewayTerminationReasonTimedOut:
            [self fatalErrorWithHeadline: @"Sync Gateway Failed To Start"
                                    text: @"For some reason, the Sync Gateway didn't finish starting up."];
        default:
            break;
    }
}


- (void) fatalErrorWithHeadline: (NSString*)headline text: (NSString*)text {
    NSAlert* alert = [NSAlert new];
    alert.messageText = headline;
    alert.informativeText = text;
    [alert addButtonWithTitle: @"Quit"];
    [alert runModal];
    [NSApp terminate: self];
}


- (IBAction) about:(id)sender {
    [NSApp orderFrontStandardAboutPanel: self];
}


- (IBAction) quit:(id)sender {
    [(NSApplication*)NSApp terminate: self];
}


- (IBAction) openAdmin: (id)sender {
    // Terminal
    // Code from John Daniel - ShellHere
    TerminalApplication * terminal = [SBApplication applicationWithBundleIdentifier: @"com.apple.Terminal"];
	BOOL terminalWasRunning = [terminal isRunning];

    // Get the Terminal windows.
    SBElementArray * terminalWindows = [terminal windows];

    TerminalTab * currentTab = nil;

    // If there is only a single window with a single tab, Terminal may
    // have been just launched. If so, I want to use the new window.
	// (This prevents two windows from being created.)
    if(!terminalWasRunning) {
        for(TerminalWindow * terminalWindow in terminalWindows) {
		    SBElementArray * windowTabs = [terminalWindow tabs];
            for(TerminalTab * tab in windowTabs) {
                currentTab = tab;
            }
        }
    }

    // Run the command.
    NSString * command = [NSString stringWithFormat: @"curl %@", _gateway.adminPort];
    [terminal doScript: command in: currentTab];

    // Activate the Terminal. Hopefully, the new window is already open and
    // is will be brought to the front.
    [terminal activate];

}


- (IBAction) viewLogs: (id)sender {
    [[NSWorkspace sharedWorkspace] openFile: _gateway.logFile];
}


- (BOOL) validateUserInterfaceItem: (id<NSValidatedUserInterfaceItem>)item {
    if (item.action == @selector(openAdmin:)) {
        return _gateway.isRunning;
    } else {
        return YES;
    }
}


@end
