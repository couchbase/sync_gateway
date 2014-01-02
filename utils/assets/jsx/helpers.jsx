/** @jsx React.DOM */

var coax = require("coax"),
  sg = coax(location.origin),
  SyncModel = require("syncModel");


  // window.addEventListener("beforeunload", function() {
  //   return "You have unsaved preview"
  // })

function dbState(db) {
  // console.log("dbState",sg(db).url)
  return SyncModel.SyncModelForDatabase(sg(db).url.toString())
}

function dbLink(db, path) {
  var base = "/_utils/db/"+db
  if (path) {
    base += "/"+path
  }
  return base
}

function channelLink(db, channel) {
  return <a href={dbLink(db,"channels/"+channel)}>{channel}</a>
}

function docLink(db, id) {
  return <a href={dbLink(db,"documents/"+id)}>{id}</a>
}

function userLink(db, user) {
  return <a href={dbLink(db,"users/"+user)}>{user}</a>
}


window.brClear = React.createClass({
  shouldComponentUpdate : function() {
    return false;
  },
  render : function() {
    return <br className="clear"/>
  }
})

window.StateForPropsMixin = {
  componentWillReceiveProps: function(newProps) {
    // console.log("StateForPropsMixin componentWillReceiveProps", newProps, this.props)
    this.setStateForProps(newProps, this.props)
  },
  componentWillMount: function() {
    // console.log("StateForPropsMixin componentWillMount", this.props)
    this.setStateForProps(this.props)
  }
};

window.EventListenerMixin = {
  listen : function(emitter, event, handler) {
    // console.log("listen", event)
    var mixinStateKeyForEvent = "_EventListenerMixinState:"+event;
    var sub = this.state[mixinStateKeyForEvent] || {};
    if (sub.event && sub.emitter) {
      if (sub.event == event && sub.emitter === emitter) {
        // we are already listening, noop
        // console.log("EventListenerMixin alreadyListening", sub.event, this)
        return;
      } else {
        // unsubscribe from the existing one
        // console.log("EventListenerMixin removeListener", sub.event, this)
        sub.emitter.removeListener(sub.event, sub.handler)
      }
    }
    var mixinState = {
      emitter : emitter,
      event : event,
      handler : handler
    }
    // console.log("EventListenerMixin addListener", event, this, mixinState)
    var stateToMerge = {};
    stateToMerge[mixinStateKeyForEvent] = mixinState;
    this.setState(stateToMerge);
    emitter.on(event, handler)
  },
  componentWillUnmount : function() {
    // console.log("componentWillUnmount", JSON.stringify(this.state))
    for (var eventKey in this.state) {
      var ekps = eventKey.split(":")
      if (ekps[0] == "_EventListenerMixinState") {
        var sub = this.state[eventKey]
        var emitter = sub.emitter
        // console.log("EventListenerMixin Unmount removeListener", eventKey, sub, this)
        emitter.removeListener(sub.event, sub.handler)
      }
    }
  },
}
