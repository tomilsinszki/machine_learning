source: http://www.fast.ai/2017/11/13/validation-sets/

### Why have a validation set?

Some machine learning models can be impressive in development, but can deliver poor results when used with production data. To avoid this from happening we need to make sure we are using a good validation set.

### Why have different sets?

**Training Set:** Used to train a given model

**Validtion Set:** Used to choose between models (which one delivers better results for your problem? a neural net? a random forest with 40 trees? or maybe a random forest with 50 trees? etc)

**Test Set:** Used to make sure that your model works and check how well it works. It's possible that your model works well for the validation set only by chance.

### How to create sets when dealing with time series

If your data is a time series, choosing a random subset of the data will not represent well the new data that you will see.

If your data includes a date and you are building a model to use in the future, you will want to choose a continuous section with the latest dates as your validation set (for instance, the last two weeks or last month of the available data).

Use earlier data as the training set and the later data for the validation set.

The validation set should be the same length as the new data that you will run the model on.

### Qualitative differences in production data

If you have a model that predicts what a person in a picture is doing, then you would want to make sure you are using different people in testing and different people for the validation set.

Having the same people in both testing and the validation set could cause the model to perform better, because it would recognise something about the person in the picture, etc.

Sometimes it may not be clear how your test data will differ. For instance, for a problem using satellite imagery, you’d need to gather more information on whether the training set just contained certain geographic locations, or if it came from geographically scattered data.

### When to use (and not to use) cross-validation

**3-Fold Cross Validation:** Data is divided into 3 sets: A, B and C. A model is first trained on A and B combined as the training set, and evaluated on the validation set C. Next, a model is trained on A and C combined as the training set, and evaluated on validation set B. And so on, with the model performance from the 3 folds being averaged in the end.

This is only a useful method if you're data can be randomly shuffled around.

But it's not a good idea to use cross validation for time series data for example.


