## 🔁 Git 커밋 & 푸시 워크플로우

```bash
git add "Field Ride Coaching_250801.sql"
git commit -m "Add coaching query for 2025-08-01"
git push origin main  ← 이걸 해줘야 GitHub에 올라감!



## 📌 Git 커밋 메시지 입력 (COMMIT_EDITMSG 상태)

🟡 이 화면은 Git이 커밋 메시지를 입력하라고 텍스트 편집기(Vim or VS Code)를 열었을 때 표시됨.

✅ 해야 할 것:
1. 가장 윗줄 (1번 줄)에 커밋 메시지를 입력
   예시:
   - Add: git workflow guide
   - Fix: removed outdated coaching SQL
   - Update: modified query logic

2. 저장 (Ctrl + S)

3. 종료
   - VS Code: Ctrl + W
   - 터미널 Vim: Esc → `:wq` + Enter
