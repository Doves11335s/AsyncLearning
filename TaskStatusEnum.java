package com.founder.enums;

public enum TaskStatusEnum implements BaseEnum<String> {
    PENDING("PENDING", "未执行"),
    RUNNING("RUNNING", "执行中"),
    SUCCESS("SUCCESS", "执行成功"),
    FAILED("FAILED", "执行失败"),
    INTERRUPTED("INTERRUPTED", "执行中断");

    private final String code;
    private final String description;

    TaskStatusEnum(String code, String description) {
        this.code = code;
        this.description = description;
    }

    // Getter方法
    public String getCode() {
        return code;
    }

    @Override
    public String getDesc() {
        return this.description;
    }

    public String getDescription() {
        return description;
    }

    // 根据code获取枚举
    public static TaskStatusEnum fromCode(String code) {
        for (TaskStatusEnum status : values()) {
            if (status.code.equals(code)) {
                return status;
            }
        }
        throw new IllegalArgumentException("未知的任务状态: " + code);
    }
}