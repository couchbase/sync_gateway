/**
 * @jsx React.DOM
 */

function draw(component, container) {
  React.renderComponent(
    component,
    container || document.getElementById('container')
  );
}

Davis.$ = Zepto;
Davis(function() {
  this.settings.generateRequestOnPageLoad = true;
  this.settings.handleRouteNotFound = true;

  /*  404 handler
      If the 404 is in-app, redirect to the index page.
  */
  this.bind("routeNotFound", function(r) {
    setTimeout(function(){ // required sleep
      window.location = "/_utils"
    },100)
  })

  /*  External link handler
      Fallback to a new page load.
  */
  this.bind("lookupRoute", function(req) {
    // console.log("lookupRoute", req.path, req.path.indexOf("/_utils"))
    // alert(req.path)
    if (req.path.indexOf("/_utils") !== 0) {
      // console.log("delegateToServer")
      window.location = req.path;
      req.delegateToServer()
    }
  })

  // trim the path-prefix
  this.scope("/_utils", function() {

    /*  /_utils/
        The home page, list and create databases.
    */
    this.get('/', function (req) {
      draw(
        <PageWrap page="home">
          <p>Welcome to Couchbase Sync Gateway. You are connected to the admin port at <a href={location.toString()}>{location.toString()}</a></p>
          <AllDatabases title="Please select a database:"/>
          <p>Link to docs. Architecture diagram. cloud signup, downloads. Click here to install sample datasets: beerdb, todos, </p>
        </PageWrap>)
    })


    /*  /_utils/db/myDatabase
        /_utils/db/myDatabase/documents/myDocID
        The index page for myDatabase, list and edit documents.
    */
    function docIndex(req) {
          draw(
            <PageWrap db={req.params.db} page="documents">
              <DocumentsPage db={req.params.db} docID={req.params.id}/>
            </PageWrap>);
        }
    this.get('/db/:db', docIndex)
    // old: this.get('/db/:db/documents', docIndex)
    this.get('/db/:db/documents/:id', docIndex)


    /*  /_utils/db/myDatabase/sync
        Sync function editor for myDatabase
    */
    this.get('/db/:db/sync', function (req) {
      draw(
        <PageWrap db={req.params.db} page="sync">
          <SyncPage db={req.params.db}/>
        </PageWrap>);
    })

    /*  /_utils/db/myDatabase/channels
        Channel watcher page for myDatabase

        /_utils/db/myDatabase/channels/myChannel
        Channel detail page
    */
    this.get('/db/:db/channels', function (req) {
      var watch = (req.params.watch && req.params.watch.split(',') || []);
      draw(
        <PageWrap db={req.params.db} page="channels">
            <ChannelsWatchPage db={req.params.db} watch={watch} title={req.params.title}/>
        </PageWrap>);
    })
    this.get('/db/:db/channels/:id', function (req) {
      draw(
        <PageWrap db={req.params.db} page="channels">
          <ChannelInfoPage db={req.params.db} id={req.params.id}/>
        </PageWrap>);
    })

    /*  /_utils/db/myDatabase
        /_utils/db/myDatabase/documents/myDocID
        List and edit users.
    */
    function userPage(req) {
      draw(
        <PageWrap page="users" db={req.params.db}>
          <UsersPage db={req.params.db} userID={req.params.id}/>
        </PageWrap>);
    }
    this.get('/db/:db/users', userPage)
    this.get('/db/:db/users/:id', userPage)
    // todo this.get('/db/:db/users/:id/channels', userPage)
  })
});

