# crypto-bot

### version 1

## 構造
・メインプロセス<br>
・WebSocketプロセス<br>
・ログ書き込みプロセス<br>

## MessageHandlerクラス
・WebSocketプロセスから送られてくる板情報、約定情報を保持する。約定情報はn秒のローソク足にして保持。<br>
・メインプロセスはこのクラスの保持する情報を参照して発注する<br>

## Parasクラス
・パラメータを指定する<br>

## 処理フロー
(0).メインプロセスが他プロセスをキックする。MessageHandlerクラスに情報が溜まり始める<br>
(1).スプレッドが一定以上ならロングとショート両方の指値注文を入れる。<br>
(2).1秒待つ<br>
(3).両方約定していれば１に戻る。<br>
(4).両方未約定でスプレッドが一定以上なら注文の価格をbest_priceに合わせて編集し、2に戻る。<br>
(5).片方のみ約定なら、未約定の注文の価格をbest_priceに合わせて編集する。これを約定するまで続け、約定すれば(1)に戻る。<br>
    ただし期待リターンが低い場合、ポジションをクローズして(1)に戻る。<br>

## 改善すること
・remainingを考慮する<br>
・現在は一回のエントーで必ず両方決済させているが、残ってもいいので次のエントリーをする。在庫管理。<br>
・決済ロジックを改善<br>
・指値の値は機械的に算出しているが、値動きを予測し、いい感じのところに指値を置くようにする。<br>
