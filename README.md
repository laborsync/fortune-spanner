# Fortune Google Cloud Spanner Adapter

This is a [Google Cloud Spanner](https://cloud.google.com/spanner/) adapter for [Fortune](http://fortune.js.org).

## Usage

Install the `fortune-spanner` package from `npm`:

```git 
$ npm install laborsync/fortune-spanner
```

Then use it with Fortune:

```js
import fortune from 'fortune'
import spannerAdapter from 'fortune-spanner'

const store = fortune({...},  {
  adapter: [
    spannerAdapter,
    {
      projectId: 'spanner-adapter-test',
      instanceId: 'spanner-test',
      databaseId: 'test',
      typeMap: {
        account: {
          table: 'Accounts'
        },
        user: {
          table: 'Users',
          interleave: 'account'
        }
      }
    }
  ]
})
```

## Adapter Options


Option    | Description
:------------- | :-------------
generateId   | Generate the id key on a new record. It must be a function that accepts one argument, the record type, and returns a unique string or number. Optional.
projectId | The project ID from the Google Developer's Console, e.g. 'green-kangaroo-1337'. We will also check the environment variable GCLOUD_PROJECT for your project ID. If your app is running in an environment which supports [Application Default Credentials](https://developers.google.com/identity/protocols/application-default-credentials), your project ID will be detected automatically.
instanceId | The instance ID from the Google Developer Console.
databaseId | The database ID within the instance from the Google Developer Console.
typeMap| An object keyed by type name with value of an object. The type name object is keyed by table and optional interleave type.

## License

This software is licensed under the MIT License.