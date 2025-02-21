.. _sync_to_async_plugins:

Running Sync Plugins Async
==========================================
In some cases, you may want to use a specific engine like ``pandas`` or ``pyspark`` 
that does not support async operations natively. This can be a problem when running multiple tasks concurrently, 
as it can block the asyncio event loop and cause the entire system to run synchronously.

**Solution**
To prevent the event loop from blocking, you can convert synchronous functions into asynchronous ones using 
the ``AsyncAdapterMixin``. One of the key benefits of this mixin is that it allows you to wrap a function into an async function,
offloading the task to a separate thread and preventing the event loop from blocking.

**Example**
The ``pandas.read_csv`` function is sychronous and does not support asynchronous execution out of the box. 

.. code:: python

  >>> class SimplePlugin(IExtractPlugin, AsyncAdapterMixin):
  >>>  
  >>>   async def __call__(self):
  >>>       data = ...  # Load data from source
  >>>       result = await self.async_wrap(pd.read_csv, io.StringIO(data))  #  Wrap the synchronous function into an async function
  >>>       return result