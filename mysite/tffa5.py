
import numpy as np
import pandas as pd
import tensorflow as tf

# 设置日志级别为ERROR
tf.logging.set_verbosity(tf.logging.ERROR)

class TensorFa:
    def __init__(self, files=r'D:\tools\Tools\October_2018\2018-10-24\fa5ks.csv'):
        # 数据集切片
        data2=pd.read_csv(files)
        data = pd.DataFrame()
        avg = (data2['2']+data2['3']+data2['4']+data2['5'])/4
        data['1'] = avg/data2['6']
        data['2'] = avg/data2['7']
        data['3'] = avg/data2['8']
        data['4'] = data2['9']
        data['5'] = data2['10'].apply(lambda x: x if x>=0 else 2)

        # 打乱数据集的顺序
        data = data.sample(frac=1).reset_index(drop=True)

        # 把训练集与测试集分开
        ind = int(len(data)*0.9)
        data_train,data_test = data[:ind],data[ind:]
        self.train_x, self.train_y = data_train, data_train.pop('5')
        self.test_x,self.test_y = data_test,data_test.pop('5')


        # 提取特征值
        feature_columns = [tf.feature_column.numeric_column(key=key) for key in self.train_x.keys()]

        # 构建训练模型
        self.classifier = tf.estimator.DNNClassifier(
            # 这个模型接受哪些输入的特征
            feature_columns=feature_columns,
            # 包含两个隐藏层，每个隐藏层包含10个神经元.
            hidden_units=[10, 10],
            # 最终结果要分成几类
            n_classes=3)  # ,model_dir='./tensor'

        def train_func(train_x,train_y):
            dataset=tf.data.Dataset.from_tensor_slices((dict(train_x), train_y))
            dataset = dataset.shuffle(1000).repeat().batch(100)
            return dataset


        # 进行模型训练，进行20000 个回合的训练，每次100调数据
        self.classifier.train(
            input_fn=lambda:train_func(self.train_x,self.train_y),
            steps=3000)


    # 模型预测
    def eval_input_fn(self,features, labels, batch_size):
        features=dict(features)
        if labels is None:
            # No labels, use only features.
            inputs = features
        else:
            inputs = (features, labels)
        dataset = tf.data.Dataset.from_tensor_slices(inputs)

        assert batch_size is not None, "batch_size must not be None"
        dataset = dataset.batch(batch_size)
        return dataset

    def test(self):
        predict_arr = []
        predictions = self.classifier.predict(
                input_fn=lambda:self.eval_input_fn(self.test_x,labels=self.test_y,batch_size=100))
        for predict in predictions:
            predict_arr.append(predict['probabilities'].argmax())
        result = predict_arr == self.test_y
        result1 = [w for w in result if w == True]
        print("准确率为 %s"%str((len(result1)/len(result))))

    def predict_line(self,line):
        p = pd.DataFrame([line], columns=['1', '2', '3', '4'])
        pr = self.classifier.predict(input_fn=lambda: self.eval_input_fn(p, labels=None, batch_size=100))
        return next(pr)['probabilities'].argmax()



# 保存模型
# def _serving_input_receiver_fn():
#     feature_placeholders = {k: tf.placeholder(tf.float64, [None]) for k in train_x.keys()}
#     features = { key: tf.expand_dims(tensor, -1) for key, tensor in feature_placeholders.items()}
#     return tf.contrib.learn.utils.input_fn_utils.InputFnOps(
#         features,
#         None,
#         feature_placeholders
#     )

# classifier.export_savedmodel(export_dir_base="./tensor",serving_input_receiver_fn=_serving_input_receiver_fn)


if __name__ == '__main__':
    ten=TensorFa()
    ten.test()
    result = ten.predict_line([0.999614, 1.00117, 1.003525, -1.32])
    print(result)
