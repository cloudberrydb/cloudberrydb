package cn.hashdata.dlagent.service.controller;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

/**
 * Represents a result of executing an operation. Holds operation statistics and the exception if an error
 * occurred during the operation. A caller is advised to review the statistics and re-throw the exception
 * if it was reported by the operation.
 */
@Getter
@Setter
@NoArgsConstructor
public class OperationResult {
    private Exception exception;
    private OperationStats stats;
    private String sourceName;
}
