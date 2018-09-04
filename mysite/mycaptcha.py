from PIL import Image
import numpy as np
import tensorflow as tf
# os.environ['TF_CPP_MIN_LOG_LEVEL']='2'
import string



class Captcha:
    _singleton = None
    check = None

    def __new__(cls, *args, **kwargs):
        if not cls._singleton:
            cls._singleton = super(Captcha, cls).__new__(cls)
        return cls._singleton

    def __init__(self,model_path='myfile\\captcha_model95'):
        if not self.check:
            self.model_path = model_path
            self.IMAGE_HEIGHT = 25
            self.IMAGE_WIDTH = 96
            self.MAX_CAPTCHA = 6
            self.CHAR_SET_LEN = 62  # 有多少种可能的字符
            self.keep_prob = tf.placeholder(tf.float32)
            self.X = tf.placeholder(tf.float32, [None, self.IMAGE_HEIGHT * self.IMAGE_WIDTH])
            self.Y = tf.placeholder(tf.float32, [None, self.MAX_CAPTCHA * self.CHAR_SET_LEN])
            self.check = self.crack_captcha()
            self.check.send(None)

    def get_name_and_image(self, img_path):
        image = Image.open(img_path)
        image = image.convert('L')  # 灰度化
        image = np.array(image)
        return image

    def vec2name(self, vec):
        name = []
        STR = string.printable[:62]
        for i in vec:
            a = STR[i]
            name.append(a)
        return "".join(name)

    # 定义CNN
    def crack_captcha_cnn(self,w_alpha=0.01, b_alpha=0.1):
        IMAGE_HEIGHT = self.IMAGE_HEIGHT
        IMAGE_WIDTH = self.IMAGE_WIDTH
        MAX_CAPTCHA = self.MAX_CAPTCHA
        CHAR_SET_LEN = self.CHAR_SET_LEN
        keep_prob = self.keep_prob
        X = self.X
        Y = self.Y
        x = tf.reshape(X, shape=[-1, IMAGE_HEIGHT, IMAGE_WIDTH, 1])
        # 3 conv layer
        w_c1 = tf.Variable(w_alpha * tf.random_normal([5, 5, 1, 32]))
        b_c1 = tf.Variable(b_alpha * tf.random_normal([32]))
        conv1 = tf.nn.relu(tf.nn.bias_add(tf.nn.conv2d(x, w_c1, strides=[1, 1, 1, 1], padding='SAME'), b_c1))
        conv1 = tf.nn.max_pool(conv1, ksize=[1, 2, 2, 1], strides=[1, 2, 2, 1], padding='SAME')
        conv1 = tf.nn.dropout(conv1, keep_prob)

        w_c2 = tf.Variable(w_alpha * tf.random_normal([5, 5, 32, 64]))
        b_c2 = tf.Variable(b_alpha * tf.random_normal([64]))
        conv2 = tf.nn.relu(tf.nn.bias_add(tf.nn.conv2d(conv1, w_c2, strides=[1, 1, 1, 1], padding='SAME'), b_c2))
        conv2 = tf.nn.max_pool(conv2, ksize=[1, 2, 2, 1], strides=[1, 2, 2, 1], padding='SAME')
        conv2 = tf.nn.dropout(conv2, keep_prob)

        w_c3 = tf.Variable(w_alpha * tf.random_normal([5, 5, 64, 64]))
        b_c3 = tf.Variable(b_alpha * tf.random_normal([64]))
        conv3 = tf.nn.relu(tf.nn.bias_add(tf.nn.conv2d(conv2, w_c3, strides=[1, 1, 1, 1], padding='SAME'), b_c3))
        conv3 = tf.nn.max_pool(conv3, ksize=[1, 2, 2, 1], strides=[1, 2, 2, 1], padding='SAME')
        conv3 = tf.nn.dropout(conv3, keep_prob)

        # Fully connected layer
        w_d = tf.Variable(w_alpha * tf.random_normal([4 * 12 * 64, 1024]))
        b_d = tf.Variable(b_alpha * tf.random_normal([1024]))
        dense = tf.reshape(conv3, [-1, w_d.get_shape().as_list()[0]])
        dense = tf.nn.relu(tf.add(tf.matmul(dense, w_d), b_d))
        dense = tf.nn.dropout(dense, keep_prob)

        w_out = tf.Variable(w_alpha * tf.random_normal([1024, MAX_CAPTCHA * CHAR_SET_LEN]))
        b_out = tf.Variable(b_alpha * tf.random_normal([MAX_CAPTCHA * CHAR_SET_LEN]))
        out = tf.add(tf.matmul(dense, w_out), b_out)
        return out

    def crack_captcha(self):
        keep_prob = self.keep_prob
        output = self.crack_captcha_cnn()
        X = self.X
        saver = tf.train.Saver()
        predict_text = ''
        with tf.Session() as sess:
            saver.restore(sess, tf.train.latest_checkpoint(self.model_path))
            while 1:
                img_path = yield predict_text
                image = self.get_name_and_image(img_path)
                image = 1 * (image.flatten())
                predict = tf.argmax(tf.reshape(output, [-1, self.MAX_CAPTCHA, self.CHAR_SET_LEN]), 2)
                text_list = sess.run(predict, feed_dict={X: [image], keep_prob: 1})
                vec = text_list[0].tolist()
                predict_text = self.vec2name(vec)

        return predict_text



if __name__ == '__main__':

    img_path = r'D:\tools\Tools\September_2018\2018-9-3\test_captcha\\'
    captcha = Captcha()
    c = captcha.crack_captcha()
    c.send(None)
    print(c.send(img_path + 'p6bmpg.jpg'))
    print(c.send(img_path + 'd5rrhm.jpg'))
    # captcha.test(img_path)